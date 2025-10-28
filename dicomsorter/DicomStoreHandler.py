import os
import logging
from .src.global_var import BASE_DIR
from pynetdicom import AE
import uuid
from datetime import datetime
from .query import INSERT_QUERY_DICOM_META, INSERT_QUERY_DICOM_ASS, \
    UNIQUE_UID_SELECT
from .src.dicom_data import return_dicom_data, create_folder
from .src.global_var import SCP_AE_TITLE, QUEUE_NAME, RABBITMQ_URL
import pika
import threading
import time


class DicomStoreHandler:
    """Handles incoming DICOM C-STORE requests and saves metadata and
     association information to the database to the database."""

    def __init__(self, db):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.connection_rmq = None
        self.channel = None
        self.stop_heartbeat = threading.Event()

        with open("dicomsorter/uuids.txt") as f:
            self.valid_uuids = [line.strip() for line in f if line.strip()]
            

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

        self.channel.queue_declare(queue=QUEUE_NAME, passive=False, durable=True)
        heartbeat_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
        heartbeat_thread.start()

    def send_to_queue(self, message):
        # Convert Python dict to JSON string

        self.channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=message.encode('utf-8'),  # Convert to bytes
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent messages
        )

    def check_uid_db(self, study_uid):
        """

        :param study_uid:
        :return:
        """
        result = self.db.fetch_all(UNIQUE_UID_SELECT, params=study_uid)
        if not result:
            try:
                self.send_to_queue(study_uid)
                logging.info(f"Inserting {study_uid} in the queue")
            except:
                logging.warning("Inserting in the queue failed.")
                raise
        logging.info(f"Insertion queue complete")

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
        for uid in event.assoc.list_uid:
            self.send_to_queue(uid)

    def handle_store(self, event):
        """Receives and stores DICOM images while logging metadata to the database."""
        logging.info("Handle store ")
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id

        patient_id, study_uid, series_uid, modality, sop_uid, sop_class_uid, \
            instance_number, modality_type, referenced_rt_plan_uid, referenced_sop_class_uid = return_dicom_data(ds)

        if study_uid not in self.valid_uuids: #Check if the study ID is in the uuids.txt if not dont release
            logging.warning(f"{study_uid} is not part of the expected studies. {study_uid} did not get saved in the postgres.")
            self.delete_assoc(assoc_id)
            return 0xA700
        logging.info(f"{study_uid} is found in the expected studies.")
        
        
        filename = create_folder(patient_id, study_uid, modality, sop_uid)
        logging.info(f"Folder structure create. Saving in {filename}")
        ds.save_as(filename, write_like_original=False)

        logging.info(f"[INFO] Stored {modality} file for Patient {patient_id}: {filename}")
        # filename = filename.replace("./data/", "/Users/alessioromita/Documents/data_test_docker/")
        params = (
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

        event.assoc.list_uid.add(study_uid)

        logging.info("Inserting dicom metadata into the table")
        self.db.execute_query(INSERT_QUERY_DICOM_META, params)
        logging.info("Insert complete")
        logging.info(f"These are study uid element {event.assoc.list_uid}")

        return 0x0000
