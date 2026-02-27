import copy
import gc
import logging
import queue
import threading
from dataclasses import dataclass

from pydicom import Dataset

from .src.dicom_data import return_dicom_data, create_folder
from .query import INSERT_QUERY_DICOM_META

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 50


@dataclass
class WorkItem:
    ds: Dataset
    assoc_id: str


class BackgroundProcessor:
    def __init__(self, anonymizer, db, tracker):
        self._queue: queue.Queue[WorkItem] = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self._stop = threading.Event()
        self._anonymizer = anonymizer
        self._db = db
        self._tracker = tracker
        self._thread = threading.Thread(target=self._worker_loop, daemon=True, name="bg-processor")
        self._thread.start()
        logger.info("BackgroundProcessor started")

    def enqueue(self, ds: Dataset, assoc_id: str):
        ds_copy = copy.deepcopy(ds)
        item = WorkItem(ds=ds_copy, assoc_id=assoc_id)
        self._queue.put(item)
        logger.debug(f"Enqueued work item for assoc {assoc_id}, queue size ~{self._queue.qsize()}")

    def _worker_loop(self):
        while not self._stop.is_set():
            try:
                item = self._queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                self._process_item(item)
                self._tracker.record_processed(item.assoc_id)
            except Exception:
                logger.exception(f"Worker failed processing item for assoc {item.assoc_id}")
                self._tracker.record_error(item.assoc_id)
            finally:
                del item
                gc.collect()

        self._drain()

    def _process_item(self, item: WorkItem):
        ds = item.ds
        assoc_id = item.assoc_id

        anonymised_ds = self._anonymizer.run(ds)
        if anonymised_ds is None:
            sop_uid = getattr(ds, "SOPInstanceUID", "UNKNOWN")
            logger.error(f"Anonymization failed for SOP {sop_uid}")
            del ds
            raise RuntimeError("Anonymization returned None")

        del ds
        item.ds = None

        patient_name, patient_id, study_uid, series_uid, modality, sop_uid, sop_class_uid, \
            instance_number, modality_type, referenced_rt_plan_uid, referenced_sop_class_uid = return_dicom_data(
            anonymised_ds)

        filename = create_folder(patient_id, study_uid, modality, sop_uid)
        anonymised_ds.save_as(filename, write_like_original=False)
        logger.info(f"Stored {modality} file for patient {patient_id}: {filename}")

        del anonymised_ds

        params = (
            patient_name, patient_id, study_uid, series_uid, modality,
            sop_uid, sop_class_uid, instance_number, filename,
            referenced_rt_plan_uid, referenced_sop_class_uid, modality_type, assoc_id
        )
        self._db.execute_query(INSERT_QUERY_DICOM_META, params)

    def _drain(self):
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            try:
                self._process_item(item)
                self._tracker.record_processed(item.assoc_id)
            except Exception:
                logger.exception(f"Worker drain failed for assoc {item.assoc_id}")
                self._tracker.record_error(item.assoc_id)
            finally:
                del item
                gc.collect()

    def shutdown(self):
        logger.info("BackgroundProcessor shutting down...")
        self._stop.set()
        self._thread.join(timeout=300)
        if self._thread.is_alive():
            logger.warning("BackgroundProcessor thread did not exit in time")
        logger.info("BackgroundProcessor stopped")
