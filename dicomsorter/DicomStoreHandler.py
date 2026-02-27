import os
import logging
from .src.global_var import BASE_DIR
from pynetdicom import AE
import uuid
from datetime import datetime
from .query import INSERT_QUERY_DICOM_META, INSERT_QUERY_DICOM_ASS
from .src.dicom_data import return_dicom_data, create_folder
from .src.global_var import SCP_AE_TITLE, QUEUE_NAME, QUEUE_NAME_RADIOMCS, USE_RADIOMICS
import pika
import threading
import time

logger = logging.getLogger(__name__)


class DicomStoreHandler:
    """Handles incoming DICOM C-STORE requests and saves metadata and
     association information to the database to the database."""

    def __init__(self, db, send_to_main=True):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.connection_rmq = None
        self.channel = None
        self.stop_heartbeat = threading.Event()

        # Determine which queues to send to
        self.queues = []
        if send_to_main:
            self.queues.append(QUEUE_NAME)
        if USE_RADIOMICS:
            self.queues.append(QUEUE_NAME_RADIOMCS)

        if not self.queues:
            raise ValueError("At least one queue must be selected for sending messages.")

        uuids_file = os.path.join(os.path.dirname(BASE_DIR), "recipes", "uuids.txt")
        with open(uuids_file) as f:
            valid_uuids = [line.strip() for line in f if line.strip()]
        self.valid_uuids = valid_uuids

    def open_connection(self, rabbitmq_url):
        """Establish connection"""
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        self.connection_rmq = connection
        self.channel = self.connection_rmq.channel()

    def send_heartbeats(self):
        """Send periodic heartbeats to keep the connection alive"""
        while not self.stop_heartbeat.is_set():
            try:
                logging.info("Sending heartbeat..")
                self.channel.basic_qos(prefetch_count=1)
                logging.info("Heartbeat sent.")
            except Exception as e:
                print(f"Heartbeat error: {e}")
                raise e
            time.sleep(10)

    def close_connection(self):
        """Close connection"""

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
        logging.info(f"Sent message to queues: {QUEUE_NAME}, {QUEUE_NAME_RADIOMCS}")

    def handle_assoc_open(self, event):
        """
        Assigns a UUID to a new DICOM association and stores details.
        :param event:
        :return:
        """
        assoc_id = str(uuid.uuid4())  # Generate a unique ID
        ae_title = event.assoc.requestor.ae_title
        ae_address = event.assoc.requestor.address
        ae_port = event.assoc.requestor.port
        event.assoc.assoc_id = assoc_id
        event.assoc.list_uid = set()
        # event.assoc.patient_id = None
        event.assoc.uid_pat_id = {}

        params = (
            assoc_id,
            ae_title,
            ae_address,
            ae_port,
            datetime.now()
        )
        logging.info(f"Inserting value in Assoc table {params}")
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_assoc_close(self, event):
        for uid in event.assoc.uid_pat_id:
            self.send_to_queue(uid)

    def handle_store(self, event):
        """Receives and stores DICOM images while logging metadata to the database."""
        logging.info("Handle store ")
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id

        patient_name, patient_id, study_uid, series_uid, modality, sop_uid, sop_class_uid, \
            instance_number, modality_type, referenced_rt_plan_uid, referenced_sop_class_uid = return_dicom_data(ds)

        # Check if the study ID is in the uuids.txt if not dont release
        if self.valid_uuids:
            if study_uid not in self.valid_uuids:
                logging.warning(
                    f"Received study UID {study_uid} which is not in the allowed list. "
                    "This C-STORE request will be rejected. AE: %s:%s",
                    event.assoc.requestor.ae_title,
                    event.assoc.requestor.address
                )
                return 0xC211
        logger.info(f"{study_uid} is found in the expected studies.")

        filename = create_folder(patient_id, study_uid, modality, sop_uid)
        logging.info(f"Folder structure create. Saving in {filename}")
        ds.save_as(filename, write_like_original=False)

        logging.info(f"[INFO] Stored {modality} file for Patient {patient_id}: {filename}")

        params = (
            patient_name,
            patient_id,
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
            assoc_id
        )
        logging.info(f"Checking if {study_uid} in database before inserting into the queue and table")

        event.assoc.uid_pat_id[study_uid] = patient_id
        logging.info("Inserting dicom metadata into the table")
        self.db.execute_query(INSERT_QUERY_DICOM_META, params)
        logging.info("Insert complete")
        logging.info(f"These are study uid element {event.assoc.list_uid}")

        return 0x0000
