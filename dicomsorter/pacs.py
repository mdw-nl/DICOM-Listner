import logging
import os
import threading
from pathlib import Path

import pika
from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE, StoragePresentationContexts

from dicomsorter.queries import INSERT_PACS_ARCHIVE_PENDING, QUERY_PENDING_SOPS, UPDATE_PACS_ARCHIVE_ARCHIVED
from dicomsorter.settings import (
    PACS_QUEUE_NAME,
    PACS_SCP_AE_TITLE,
    PACS_SCP_HOST,
    PACS_SCP_PORT,
    PACS_SCU_AE_TITLE,
)

logger = logging.getLogger(__name__)


class DICOMtoPACS:
    def __init__(self):
        self.host = PACS_SCP_HOST
        self.port = PACS_SCP_PORT
        self.ae_title = PACS_SCP_AE_TITLE
        self.ae = AE(ae_title=PACS_SCU_AE_TITLE)
        for context in StoragePresentationContexts:
            self.ae.add_requested_context(context.abstract_syntax, ExplicitVRLittleEndian)

    def _iter_dicom_files(self, folder_path: str):
        for root, _, files in os.walk(folder_path):
            for name in files:
                if name.lower().endswith(".dcm"):
                    yield Path(root) / name

    def dicom_to_pacs(self, data_folder: str) -> bool:
        files = list(self._iter_dicom_files(data_folder))
        if not files:
            raise ValueError(f"No DICOM files found in {data_folder}")

        assoc = self.ae.associate(self.host, self.port, ae_title=self.ae_title)
        if not assoc.is_established:
            logger.error("Failed to associate with PACS SCP at %s:%s", self.host, self.port)
            return False

        try:
            for file_path in files:
                ds = dcmread(str(file_path), defer_size="2 MB")
                status = assoc.send_c_store(ds)
                del ds
                if not status or status.Status != 0x0000:
                    raise RuntimeError(f"C-STORE failed for {file_path.name}: status={getattr(status, 'Status', None)}")
                logger.debug("C-STORE succeeded: %s", file_path.name)
        finally:
            assoc.release()

        logger.info("PACS upload succeeded")
        return True

    def send_files(self, file_paths, on_sent=None):
        assoc = self.ae.associate(self.host, self.port, ae_title=self.ae_title)
        if not assoc.is_established:
            raise ConnectionError(f"Failed to associate with PACS SCP at {self.host}:{self.port}")
        try:
            for file_path in file_paths:
                ds = dcmread(str(file_path), defer_size="2 MB")
                sop_uid = str(ds.SOPInstanceUID)
                status = assoc.send_c_store(ds)
                del ds
                if not status or status.Status != 0x0000:
                    raise RuntimeError(
                        f"C-STORE failed for {Path(file_path).name}: status={getattr(status, 'Status', None)}"
                    )
                logger.debug("C-STORE succeeded: %s", Path(file_path).name)
                if on_sent:
                    on_sent(sop_uid)
        finally:
            assoc.release()

    def run(self, data_folder: str):
        try:
            success = self.dicom_to_pacs(data_folder)
            if success:
                logger.info("Sent DICOM files from %s to PACS", data_folder)
            else:
                logger.error("PACS upload failed for %s", data_folder)
        except Exception:
            logger.exception("PACS upload failed for %s", data_folder)


class PacsConsumer:
    def __init__(self, db, mq_url: str, sender: DICOMtoPACS, interval: int):
        self._db = db
        self._mq_url = mq_url
        self._sender = sender
        self._interval = interval
        self._stop = threading.Event()
        self._connection = None
        self._channel = None

    def start(self):
        t = threading.Thread(target=self._run_loop, daemon=True, name="pacs-consumer")
        t.start()

    def _connect(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(self._mq_url))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=PACS_QUEUE_NAME, durable=True)

    def _run_loop(self):
        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._process_batch()
            except Exception:
                logger.exception("PACS consumer batch failed; will retry next interval")
                self._connection = None

    def _process_batch(self):
        if not self._connection or not self._connection.is_open:
            self._connect()
        while True:
            method, _, body = self._channel.basic_get(queue=PACS_QUEUE_NAME, auto_ack=False)
            if method is None:
                break
            study_uid = body.decode().strip()
            try:
                self._process_study(study_uid)
                self._channel.basic_ack(method.delivery_tag)
            except Exception:
                logger.exception("PACS send failed for study %s", study_uid)
                self._channel.basic_nack(method.delivery_tag, requeue=True)

    def _process_study(self, study_uid: str):
        rows = self._db.fetch_all(QUERY_PENDING_SOPS, (study_uid,))
        if not rows:
            logger.info("All SOPs in study %s already archived", study_uid)
            return

        for sop_uid, series_uid, modality, patient_id, _ in rows:
            self._db.execute_query(INSERT_PACS_ARCHIVE_PENDING, (sop_uid, series_uid, modality, study_uid, patient_id))

        file_paths = [row[4] for row in rows if row[4]]

        def on_sent(sop_uid):
            self._db.execute_query(UPDATE_PACS_ARCHIVE_ARCHIVED, (sop_uid,))
            logger.debug("Archived SOP %s", sop_uid)

        self._sender.send_files(file_paths, on_sent=on_sent)
        logger.info("Archived %d SOPs for study %s", len(file_paths), study_uid)
