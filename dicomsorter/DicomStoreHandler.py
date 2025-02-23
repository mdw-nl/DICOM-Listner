import os
import logging
from .global_variables import BASE_DIR
from pynetdicom import AE
import uuid
from datetime import datetime
from .query import INSERT_QUERY_DICOM_META, INSERT_QUERY_DICOM_ASS
from .src.dicom_data import return_dicom_data
from .src.global_var import SCP_AE_TITLE, QUEUE_NAME, RABBITMQ_URL
import pika


class DicomStoreHandler:
    """Handles incoming DICOM C-STORE requests and saves metadata and
     association information to the database to the database."""

    def __init__(self, db):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.connection = None

    def open_connection(self):
        """Establish connection"""
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        self.connection = connection

    def close_connection(self):
        """Close connection"""
        self.connection.close()

    def create_queue(self):
        channel = self.connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, passive=True)

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

        params = (
            assoc_id,
            ae_title,
            ae_address,
            ae_port,
            datetime.now()
        )
        logging.info(f"Inserting value in Assoc table {params}")
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_store(self, event):
        """Receives and stores DICOM images while logging metadata to the database."""
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id
        patient_id, study_uid, series_uid, modality, sop_uid, sop_class_uid, \
            instance_number, modality_type = return_dicom_data(ds)

        # Create directories for storage
        patient_folder = os.path.join(BASE_DIR, patient_id, study_uid, modality)
        os.makedirs(patient_folder, exist_ok=True)

        # Save the DICOM file
        filename = os.path.join(patient_folder, f"{sop_uid}.dcm")
        ds.save_as(filename, write_like_original=False)

        logger.info(f"[INFO] Stored {modality} file for Patient {patient_id}: {filename}")
        params = (
            patient_id,
            study_uid,
            series_uid,
            modality,
            sop_uid,
            sop_class_uid,
            instance_number,
            filename,
            modality_type,
            assoc_id
        )
        logging.info(f"Inserting value in Meta table")
        self.db.execute_query(INSERT_QUERY_DICOM_META, params)

        return 0x0000
