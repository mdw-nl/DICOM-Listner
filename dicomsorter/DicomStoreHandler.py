import os
import logging
import uuid
import threading
import time
from datetime import datetime

import pika
from pynetdicom import AE

from .query import INSERT_QUERY_DICOM_META, INSERT_QUERY_DICOM_ASS
from .src.dicom_data import return_dicom_data, create_folder
from .src.global_var import (
    ANONYMIZER_QUEUE_NAME,
    BASE_DIR,
    QUEUE_NAME,
    QUEUE_NAME_RADIOMCS,
    SCP_AE_TITLE,
    USE_ANONYMIZER,
    USE_RADIOMICS,
)

logger = logging.getLogger(__name__)


class DicomStoreHandler:
    """Handles incoming DICOM C-STORE requests and stores metadata to the database."""

    def __init__(self, db, send_to_main=True):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.connection_rmq = None
        self.channel = None
        self.stop_heartbeat = threading.Event()

        self.queues = []
        if send_to_main:
            primary_queue = ANONYMIZER_QUEUE_NAME if USE_ANONYMIZER else QUEUE_NAME
            self.queues.append(primary_queue)
        if USE_RADIOMICS:
            self.queues.append(QUEUE_NAME_RADIOMCS)

        if not self.queues:
            raise ValueError("At least one queue must be selected for sending messages.")

        uuids_file = os.path.join(os.path.dirname(BASE_DIR), "recipes", "uuids.txt")
        with open(uuids_file) as f:
            self.valid_uuids = [line.strip() for line in f if line.strip()]

    def open_connection(self, rabbitmq_url):
        parameters = pika.URLParameters(rabbitmq_url)
        self.connection_rmq = pika.BlockingConnection(parameters)
        self.channel = self.connection_rmq.channel()

    def send_heartbeats(self):
        while not self.stop_heartbeat.is_set():
            try:
                logging.info("Sending heartbeat..")
                self.channel.basic_qos(prefetch_count=1)
                logging.info("Heartbeat sent.")
            except Exception as exc:
                print(f"Heartbeat error: {exc}")
                raise
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
                exchange='',
                routing_key=queue,
                body=message.encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        logging.info("Sent message to queues: %s (USE_ANONYMIZER=%s)", self.queues, USE_ANONYMIZER)

    def _generate_patient_id(self):
        return f"PAT-{uuid.uuid4().hex[:12].upper()}"

    def get_or_create_generated_patient_id(self, original_patient_id):
        existing = self.db.fetch_one(
            """
            SELECT generated_patient_id
            FROM patient_id_map
            WHERE original_patient_id = %s
            """,
            (original_patient_id,),
        )
        if existing:
            return existing[0]

        generated_patient_id = self._generate_patient_id()
        self.db.execute_query(
            """
            INSERT INTO patient_id_map (original_patient_id, generated_patient_id)
            VALUES (%s, %s)
            ON CONFLICT (original_patient_id) DO NOTHING
            """,
            (original_patient_id, generated_patient_id),
        )

        resolved = self.db.fetch_one(
            """
            SELECT generated_patient_id
            FROM patient_id_map
            WHERE original_patient_id = %s
            """,
            (original_patient_id,),
        )
        if not resolved:
            raise RuntimeError(f"Unable to resolve generated patient ID for {original_patient_id}")
        return resolved[0]

    def handle_assoc_open(self, event):
        assoc_id = str(uuid.uuid4())
        ae_title = event.assoc.requestor.ae_title
        ae_address = event.assoc.requestor.address
        ae_port = event.assoc.requestor.port
        event.assoc.assoc_id = assoc_id
        event.assoc.list_uid = set()
        event.assoc.uid_pat_id = {}

        params = (assoc_id, ae_title, ae_address, ae_port, datetime.now())
        logging.info("Inserting value in Assoc table %s", params)
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_assoc_close(self, event):
        for uid in event.assoc.uid_pat_id:
            self.send_to_queue(uid)

    def handle_store(self, event):
        logging.info("Handle store")
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id

        (
            patient_name,
            patient_id,
            study_uid,
            series_uid,
            modality,
            sop_uid,
            sop_class_uid,
            instance_number,
            modality_type,
            referenced_rt_plan_uid,
            referenced_sop_class_uid,
        ) = return_dicom_data(ds)

        if self.valid_uuids and study_uid not in self.valid_uuids:
            logging.warning(
                "Received study UID %s which is not in the allowed list. This C-STORE request will be rejected.",
                study_uid,
            )
            return 0xC211

        generated_patient_id = self.get_or_create_generated_patient_id(patient_id)

        filename = create_folder(generated_patient_id, study_uid, modality, sop_uid)
        logging.info("Folder structure created. Saving in %s", filename)
        ds.save_as(filename, write_like_original=False)

        params = (
            patient_name,
            generated_patient_id,
            study_uid,
            series_uid,
            modality,
            sop_uid,
            sop_class_uid,
            instance_number,
            filename,
            referenced_rt_plan_uid,
            referenced_sop_class_uid,
            modality_type,
            assoc_id,
        )

        event.assoc.uid_pat_id[study_uid] = generated_patient_id
        self.db.execute_query(INSERT_QUERY_DICOM_META, params)
        logging.info("Inserted dicom metadata for study %s", study_uid)
        return 0x0000
