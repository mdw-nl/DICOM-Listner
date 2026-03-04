import logging
import multiprocessing
import queue
import threading
from collections import deque
from dataclasses import dataclass
from multiprocessing.pool import AsyncResult

from pydicom import Dataset

from anonymization import Anonymizer
from dicomsorter.query import INSERT_QUERY_DICOM_META
from dicomsorter.src.dicom_data import create_folder, return_dicom_data

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 50
_POOL_MAX_WORKERS = 4
_WORKER_MAX_TASKS = 50

_mp_context = multiprocessing.get_context("fork")

_worker_anonymizer: Anonymizer | None = None


def _init_worker(path_recipes: str) -> None:
    global _worker_anonymizer
    _worker_anonymizer = Anonymizer(path_files=path_recipes)


def _anonymize_in_worker(ds: Dataset) -> Dataset | None:
    return _worker_anonymizer.run(ds)


@dataclass
class WorkItem:
    ds: Dataset | None
    assoc_id: str
    original_patient_id: str | None = None
    pixel_data: bytes | None = None


@dataclass
class _InFlightItem:
    future: AsyncResult
    assoc_id: str
    original_patient_id: str | None
    pixel_data: bytes | None
    sop_uid: str


class BackgroundProcessor:
    def __init__(self, anonymizer, db, tracker, path_recipes):
        self._queue: queue.Queue[WorkItem] = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self._stop = threading.Event()
        self._anonymizer = anonymizer
        self._db = db
        self._tracker = tracker
        self._path_recipes = path_recipes
        self._pool = self._make_pool()
        self._thread = threading.Thread(target=self._worker_loop, daemon=True, name="bg-processor")
        self._thread.start()
        logger.info("BackgroundProcessor started")

    def _make_pool(self):
        return _mp_context.Pool(
            processes=_POOL_MAX_WORKERS,
            initializer=_init_worker,
            initargs=(self._path_recipes,),
            maxtasksperchild=_WORKER_MAX_TASKS,
        )

    def enqueue(self, ds: Dataset, assoc_id: str):
        original_patient_id = getattr(ds, "PatientID", None)
        pixel_data = None
        if hasattr(ds, "PixelData"):
            pixel_data = ds.PixelData
            del ds.PixelData
        item = WorkItem(ds=ds, assoc_id=assoc_id, original_patient_id=original_patient_id, pixel_data=pixel_data)
        self._queue.put(item)
        logger.debug("Enqueued work item for assoc %s, queue size ~%s", assoc_id, self._queue.qsize())

    def _submit_item(self, item: WorkItem, in_flight: deque):
        patient_id = item.original_patient_id
        if patient_id is None or not self._anonymizer.is_patient_known(patient_id):
            sop_uid = getattr(item.ds, "SOPInstanceUID", "UNKNOWN")
            logger.error("PatientID '%s' not in lookup CSV, rejecting SOP %s", patient_id, sop_uid)
            self._tracker.record_error(item.assoc_id, patient_id)
            return
        sop_uid = getattr(item.ds, "SOPInstanceUID", "UNKNOWN")
        future = self._pool.apply_async(_anonymize_in_worker, (item.ds,))
        item.ds = None
        in_flight.append(
            _InFlightItem(
                future=future,
                assoc_id=item.assoc_id,
                original_patient_id=patient_id,
                pixel_data=item.pixel_data,
                sop_uid=sop_uid,
            )
        )

    def _collect_one(self, in_flight: deque):
        inf = in_flight.popleft()
        try:
            anonymised_ds = inf.future.get()
            if anonymised_ds is None:
                logger.error("Anonymization failed for SOP %s", inf.sop_uid)
                raise RuntimeError("Anonymization returned None")

            if inf.pixel_data is not None:
                anonymised_ds.PixelData = inf.pixel_data
                anonymised_ds["PixelData"].VR = "OW"
                inf.pixel_data = None

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
                referenced_rtstruct_sop_uid,
                referenced_ct_series_uid,
            ) = return_dicom_data(anonymised_ds)

            filename = create_folder(patient_id, study_uid, modality, sop_uid)
            anonymised_ds.save_as(filename, write_like_original=False)
            logger.info("Stored %s file for patient %s: %s", modality, patient_id, filename)

            del anonymised_ds

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
                referenced_rtstruct_sop_uid,
                referenced_ct_series_uid,
                modality_type,
                inf.assoc_id,
            )
            self._db.execute_query(INSERT_QUERY_DICOM_META, params)
            self._tracker.record_processed(inf.assoc_id, inf.original_patient_id)
        except Exception:
            logger.exception("Worker failed processing item for assoc %s", inf.assoc_id)
            self._tracker.record_error(inf.assoc_id, inf.original_patient_id)
        finally:
            del inf

    def _worker_loop(self):
        in_flight: deque[_InFlightItem] = deque()

        while True:
            if not self._stop.is_set():
                while len(in_flight) < _POOL_MAX_WORKERS:
                    try:
                        item = self._queue.get_nowait()
                        self._submit_item(item, in_flight)
                    except queue.Empty:
                        break

            if in_flight:
                self._collect_one(in_flight)
            elif self._stop.is_set():
                break
            else:
                try:
                    item = self._queue.get(timeout=1)
                    self._submit_item(item, in_flight)
                except queue.Empty:
                    continue

        self._drain(in_flight)

    def _drain(self, in_flight: deque):
        while in_flight:
            self._collect_one(in_flight)
        while True:
            try:
                item = self._queue.get_nowait()
                self._submit_item(item, in_flight)
                self._collect_one(in_flight)
            except queue.Empty:
                break

    def shutdown(self):
        logger.info("BackgroundProcessor shutting down...")
        self._stop.set()
        self._thread.join(timeout=300)
        if self._thread.is_alive():
            logger.warning("BackgroundProcessor thread did not exit in time")
        self._pool.close()
        self._pool.join()
        logger.info("BackgroundProcessor stopped")
