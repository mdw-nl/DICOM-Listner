import gc
import logging
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import pika
from pynetdicom import AE

from anonymization import Anonymizer
from dicomsorter.association_tracker import AssociationTracker
from dicomsorter.background_processor import BackgroundProcessor
from dicomsorter.query import INSERT_QUERY_DICOM_ASS
from dicomsorter.src.global_var import BASE_DIR, QUEUE_NAME, QUEUE_NAME_RADIOMCS, SCP_AE_TITLE, USE_RADIOMICS
from dicomsorter.XNAThandler import DICOMtoXNAT

logger = logging.getLogger(__name__)


class DicomStoreHandler:
    def __init__(self, db, path_recipes, send_to_main=True):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.connection_rmq = None
        self.channel = None
        self.rabbitmq_url = None
        self.stop_heartbeat = threading.Event()

        self.queues = []
        if send_to_main:
            self.queues.append(QUEUE_NAME)
        if USE_RADIOMICS:
            self.queues.append(QUEUE_NAME_RADIOMCS)

        if not self.queues:
            raise ValueError("At least one queue must be selected for sending messages.")

        self.anonymizer = Anonymizer(path_files=path_recipes)
        self.XNATsender = DICOMtoXNAT()
        uuids_file = Path(path_recipes) / "uuids.txt"
        with uuids_file.open() as f:
            valid_uuids = [line.strip() for line in f if line.strip()]
        self.valid_uuids = valid_uuids

        self.tracker = AssociationTracker(
            on_complete_callback=self._on_association_complete,
            on_patient_complete_callback=self._on_patient_complete,
        )
        self.processor = BackgroundProcessor(
            anonymizer=self.anonymizer,
            db=self.db,
            tracker=self.tracker,
            path_recipes=path_recipes,
        )

    def open_connection(self, rabbitmq_url):
        self.rabbitmq_url = rabbitmq_url
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        self.connection_rmq = connection
        self.channel = self.connection_rmq.channel()

    def send_heartbeats(self):
        while not self.stop_heartbeat.is_set():
            try:
                if self.connection_rmq and self.connection_rmq.is_open:
                    logger.info("Processing RabbitMQ heartbeat..")
                    self.connection_rmq.process_data_events(time_limit=0)
                    logger.info("Heartbeat processed.")
                else:
                    logger.warning("RabbitMQ connection is closed, attempting to reconnect...")
                    if self.rabbitmq_url:
                        self.open_connection(self.rabbitmq_url)
                        for queue in self.queues:
                            self.channel.queue_declare(queue=queue, passive=False, durable=True)
                        logger.info("Reconnected to RabbitMQ successfully.")
            except Exception:
                logger.exception("Heartbeat error. Will retry on next cycle.")
            time.sleep(10)

    def close_connection(self):
        self.connection_rmq.close()

    def create_queue(self):
        for queue in self.queues:
            self.channel.queue_declare(queue=queue, passive=False, durable=True)

        heartbeat_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
        heartbeat_thread.start()

    def send_to_queue(self, message):
        for queue in self.queues:
            self.channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=message.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        logger.info("Sent message to queues: %s", self.queues)

    def send_to_queue_threadsafe(self, message):
        def _publish():
            for q in self.queues:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=q,
                    body=message.encode("utf-8"),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            logger.info("Sent message to queues: %s", self.queues)

        self.connection_rmq.add_callback_threadsafe(_publish)

    def handle_assoc_open(self, event):
        assoc_id = str(uuid.uuid4())
        ae_title = event.assoc.requestor.ae_title
        ae_address = event.assoc.requestor.address
        ae_port = event.assoc.requestor.port
        event.assoc.assoc_id = assoc_id

        self.tracker.register(assoc_id)

        params = (assoc_id, ae_title, ae_address, ae_port, datetime.now())
        logger.debug("\n%s", "=" * 70)
        logger.debug("NEW ASSOCIATION OPENED")
        logger.debug("Association ID: %s", assoc_id)
        logger.debug("Client: %s (%s:%s)", ae_title, ae_address, ae_port)
        logger.debug("Time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.debug("%s", "=" * 70)
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_assoc_close(self, event):
        assoc_id = getattr(event.assoc, "assoc_id", None)
        if assoc_id is None:
            logger.warning("Association closed without an assoc_id")
            return

        logger.debug("\n%s", "=" * 70)
        logger.debug("ASSOCIATION CLOSED")
        logger.debug("Association ID: %s", assoc_id)
        logger.debug("Time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.debug("%s", "=" * 70)

        self.tracker.mark_closed(assoc_id)

    def handle_store(self, event):
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id

        study_uid = getattr(ds, "StudyInstanceUID", None)

        if self.valid_uuids:
            if study_uid not in self.valid_uuids:
                logger.error(
                    "REJECTED: Study UID %s not in allowed list. Client: %s@%s",
                    study_uid,
                    event.assoc.requestor.ae_title,
                    event.assoc.requestor.address,
                )
                return 0xC211

        patient_id = getattr(ds, "PatientID", None)
        self.tracker.record_file(assoc_id, patient_id)
        self.processor.enqueue(ds, assoc_id)

        return 0x0000

    def _on_patient_complete(self, assoc_id, original_patient_id):
        anon_patient_id = self.anonymizer._patient_map.get(original_patient_id)
        if anon_patient_id is None:
            logger.warning("No anonymized ID found for patient %s in assoc %s", original_patient_id, assoc_id)
            return

        query = """
            SELECT DISTINCT study_instance_uid
            FROM dicom_insert
            WHERE assoc_id = %s AND patient_id = %s
        """
        studies = self.db.fetch_all(query, (assoc_id, anon_patient_id))

        if not studies:
            logger.warning("Patient %s complete but no studies found in database", original_patient_id)
            return

        for (study_uid,) in studies:
            try:
                self.send_to_queue_threadsafe(study_uid)
                logger.info("Queued study %s for patient %s", study_uid, anon_patient_id)
            except Exception:
                logger.exception("Failed to queue study %s", study_uid)

            try:
                study_folder = str(Path(BASE_DIR) / anon_patient_id / study_uid)
                self.XNATsender.run(study_folder)
                logger.info("Sent study %s to XNAT", study_uid)
            except Exception:
                logger.exception("XNAT upload failed for study %s", study_uid)

        gc.collect()

    def _on_association_complete(self, assoc_id, state):
        logger.info(
            "Association %s complete — processed=%s, errors=%s",
            assoc_id,
            state.processed_count,
            state.error_count,
        )
        if state.error_count > 0:
            logger.warning("Association %s finished with %s errors", assoc_id, state.error_count)
