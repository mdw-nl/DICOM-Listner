import logging
import os
from pathlib import Path

from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE, StoragePresentationContexts

from dicomsorter.settings import PACS_SCP_AE_TITLE, PACS_SCP_HOST, PACS_SCP_PORT, PACS_SCU_AE_TITLE

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

    def run(self, data_folder: str):
        try:
            success = self.dicom_to_pacs(data_folder)
            if success:
                logger.info("Sent DICOM files from %s to PACS", data_folder)
            else:
                logger.error("PACS upload failed for %s", data_folder)
        except Exception:
            logger.exception("PACS upload failed for %s", data_folder)
