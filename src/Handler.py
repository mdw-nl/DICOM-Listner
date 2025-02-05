import os
import logging
from src.Global import BASE_DIR
from pynetdicom import AE

logger = logging.getLogger(__name__)

SCP_AE_TITLE = "MY_SCP"


class DICOMStoreHandler:
    """Handles incoming DICOM C-STORE requests and saves metadata to the database."""

    def __init__(self, db):
        self.db = db  # Store database connection
        self.ae = AE(ae_title=SCP_AE_TITLE)

    def handle_store(self, event):
        """Receives and stores DICOM images while logging metadata to the database."""
        ds = event.dataset
        ds.file_meta = event.file_meta

        # Extract key DICOM attributes
        patient_id = ds.PatientID if "PatientID" in ds else "UNKNOWN"
        study_uid = ds.StudyInstanceUID if "StudyInstanceUID" in ds else "UNKNOWN"
        modality = ds.Modality if "Modality" in ds else "UNKNOWN"
        sop_uid = ds.SOPInstanceUID if "SOPInstanceUID" in ds else "UNKNOWN"

        # Create directories for storage
        patient_folder = os.path.join(BASE_DIR, patient_id, study_uid, modality)
        os.makedirs(patient_folder, exist_ok=True)

        # Save the DICOM file
        filename = os.path.join(patient_folder, f"{sop_uid}.dcm")
        ds.save_as(filename, write_like_original=False)

        logger.info(f"[INFO] Stored {modality} file for Patient {patient_id}: {filename}")
        return 0x0000

        # Insert metadata into database
        # cursor = self.db_conn.cursor()
        # cursor.execute("""
        #    INSERT INTO dicom_insert
        #    (timestamp, patient_id, study_instance_uid, series_instance_uid, modality, sop_instance_uid, file_path)
        #    VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)
        # """, (patient_id, study_uid, ds.SeriesInstanceUID, modality, sop_uid, filename))
        # self.db_conn.commit()
#
# Success response
