import logging
from pathlib import Path

from pydicom import dcmread
from pynetdicom import AE, StoragePresentationContexts

from dicomsorter.src.global_var import XNAT_SCP_AE_TITLE, XNAT_SCP_HOST, XNAT_SCP_PORT

logger = logging.getLogger(__name__)


class DICOMtoXNAT:
    def __init__(self):
        self.host = XNAT_SCP_HOST
        self.port = XNAT_SCP_PORT
        self.ae_title = XNAT_SCP_AE_TITLE
        self.ae = AE()
        self.ae.requested_contexts = StoragePresentationContexts

    def check_for_subfolders(self, folder_path: str):
        folder = Path(folder_path)
        entries = list(folder.iterdir())
        if not entries:
            raise ValueError(f"Folder is empty: {folder_path}")

        has_files = False
        subfolders = []
        for entry in entries:
            if entry.is_file():
                has_files = True
            elif entry.is_dir():
                subfolders.append(str(entry))

        if has_files and subfolders:
            raise ValueError(f"Folder contains both files and subfolders: {folder_path}")

        return [folder_path] if has_files else subfolders

    def dicom_to_xnat(self, data_folder: str) -> bool:
        folder = Path(data_folder)
        files = [f for f in folder.iterdir() if f.suffix.lower() == ".dcm"]
        if not files:
            raise ValueError(f"No DICOM files found in {data_folder}")

        assoc = self.ae.associate(self.host, self.port, ae_title=self.ae_title)
        if not assoc.is_established:
            logger.error("Failed to associate with XNAT SCP at %s:%s", self.host, self.port)
            return False

        errors = 0
        try:
            for file_path in files:
                ds = dcmread(str(file_path))
                status = assoc.send_c_store(ds)
                del ds
                if status and status.Status == 0x0000:
                    logger.debug("C-STORE succeeded: %s", file_path.name)
                else:
                    logger.error("C-STORE failed for %s: status=%s", file_path.name, status)
                    errors += 1
        finally:
            assoc.release()

        if errors == 0:
            logger.info("XNAT upload succeeded")
            return True
        logger.error("XNAT upload finished with %s errors", errors)
        return False

    def run(self, data_folder: str):
        data_folders = self.check_for_subfolders(data_folder)
        for folder in data_folders:
            try:
                success = self.dicom_to_xnat(folder)
                if success:
                    logger.info("Sent DICOM files from %s to XNAT", folder)
                else:
                    logger.error("XNAT upload failed for %s", folder)
            except Exception:
                logger.exception("XNAT upload failed for %s", folder)
