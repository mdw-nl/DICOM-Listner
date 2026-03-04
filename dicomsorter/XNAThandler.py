import os
import logging
from pydicom import dcmread
from pynetdicom import AE, StoragePresentationContexts
from .src.global_var import XNAT_SCP_HOST, XNAT_SCP_PORT, XNAT_SCP_AE_TITLE

logger = logging.getLogger(__name__)


class DICOMtoXNAT:
    def __init__(self):
        self.host = XNAT_SCP_HOST
        self.port = XNAT_SCP_PORT
        self.ae_title = XNAT_SCP_AE_TITLE
        self.ae = AE()
        self.ae.requested_contexts = StoragePresentationContexts

    def check_for_subfolders(self, folder_path: str):
        entries = os.listdir(folder_path)
        if not entries:
            raise ValueError(f"Folder is empty: {folder_path}")

        has_files = False
        subfolders = []
        for entry in entries:
            full_path = os.path.join(folder_path, entry)
            if os.path.isfile(full_path):
                has_files = True
            elif os.path.isdir(full_path):
                subfolders.append(full_path)

        if has_files and subfolders:
            raise ValueError(f"Folder contains both files and subfolders: {folder_path}")

        return [folder_path] if has_files else subfolders

    def dicom_to_xnat(self, data_folder: str) -> bool:
        files = [f for f in os.listdir(data_folder) if f.lower().endswith('.dcm')]
        if not files:
            raise ValueError(f"No DICOM files found in {data_folder}")

        assoc = self.ae.associate(self.host, self.port, ae_title=self.ae_title)
        if not assoc.is_established:
            logger.error(f"Failed to associate with XNAT SCP at {self.host}:{self.port}")
            return False

        errors = 0
        try:
            for filename in files:
                file_path = os.path.join(data_folder, filename)
                ds = dcmread(file_path)
                status = assoc.send_c_store(ds)
                del ds
                if status and status.Status == 0x0000:
                    logger.debug(f"C-STORE succeeded: {filename}")
                else:
                    logger.error(f"C-STORE failed for {filename}: status={status}")
                    errors += 1
        finally:
            assoc.release()

        if errors == 0:
            logger.info("XNAT upload succeeded")
            return True
        logger.error(f"XNAT upload finished with {errors} errors")
        return False

    def run(self, data_folder: str):
        data_folders = self.check_for_subfolders(data_folder)
        for folder in data_folders:
            try:
                success = self.dicom_to_xnat(folder)
                if success:
                    logger.info(f"Sent DICOM files from {folder} to XNAT")
                else:
                    logger.error(f"XNAT upload failed for {folder}")
            except Exception:
                logger.exception(f"XNAT upload failed for {folder}")
