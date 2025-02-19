import os
import logging
from .global_variables import BASE_DIR
from pynetdicom import AE
import uuid
from datetime import datetime
from .query import INSERT_QUERY_DICOM_META, INSERT_QUERY_DICOM_ASS

logger = logging.getLogger(__name__)

SCP_AE_TITLE = "MY_SCP"


class DicomStoreHandler:
    """Handles incoming DICOM C-STORE requests and saves metadata to the database."""

    def __init__(self, db):
        self.db = db  # Store database connection
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.ds = None
        self.event = None

    def handle_assoc_open(self, event):
        """Assigns a UUID to a new DICOM association and stores details."""
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
        self.ds = event.dataset
        self.ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id
        # Extract key DICOM attributes
        patient_id = self.ds.PatientID if "PatientID" in self.ds else "UNKNOWN"
        study_uid = self.ds.StudyInstanceUID if "StudyInstanceUID" in self.ds else "UNKNOWN"
        series_uid = self.ds.SeriesInstanceUID if "SeriesInstanceUID" in self.ds else "UNKNOWN"
        modality = self.ds.Modality if "Modality" in self.ds else "UNKNOWN"
        sop_uid = self.ds.SOPInstanceUID if "SOPInstanceUID" in self.ds else "UNKNOWN"
        sop_class_uid = self.ds.SOPClassUID if "SOPClassUID" in self.ds else "UNKNOWN"
        instance_number = int(self.ds.InstanceNumber) if "InstanceNumber" in self.ds else "UNKNOWN"
        modality_type = self.ds.get("ModalityType", "UNKNOWN")  # If ModalityType exists



        # Create directories for storage
        patient_folder = os.path.join(BASE_DIR, patient_id, study_uid, modality)
        os.makedirs(patient_folder, exist_ok=True)

        # Save the DICOM file
        filename = os.path.join(patient_folder, f"{sop_uid}.dcm")
        self.ds.save_as(filename, write_like_original=False)

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
        logging.info(f"Inserting value in Meta table {params}")
        self.db.execute_query(INSERT_QUERY_DICOM_META, params)

        return 0x0000

#
# Success response
