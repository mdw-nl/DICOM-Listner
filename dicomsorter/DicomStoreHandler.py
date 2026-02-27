import os
import logging
from .src.global_var import BASE_DIR
from pynetdicom import AE
import uuid
from datetime import datetime
from .query import INSERT_QUERY_DICOM_ASS
from .src.global_var import SCP_AE_TITLE, QUEUE_NAME, QUEUE_NAME_RADIOMCS, USE_RADIOMICS
import pika
import threading
import time
import gc
from anonymization import Anonymizer
from .XNAThandler import DICOMtoXNAT
from .association_tracker import AssociationTracker
from .background_processor import BackgroundProcessor

logger = logging.getLogger(__name__)


class DicomStoreHandler:

    def __init__(self, db, path_recipes, send_to_main=True):
        self.db = db
        self.ae = AE(ae_title=SCP_AE_TITLE)
        self.connection_rmq = None
        self.channel = None
        self.rabbitmq_url = None
        self.stop_heartbeat = threading.Event()

        self.queues = []
        if send_to_main:
            self.queues.append(QUEUE_NAME)
        if USE_RADIOMICS:
            self.queues.append(QUEUE_NAME_RADIOMCS)

        if not self.queues:
            raise ValueError("At least one queue must be selected for sending messages.")

        self.anonymizer = Anonymizer(path_files=path_recipes)
        treat_file = os.path.join(path_recipes, "treatment.csv")
        self.XNATsender = DICOMtoXNAT(treatment_path=treat_file)
        uuids_file = os.path.join(path_recipes, "uuids.txt")
        with open(uuids_file) as f:
            valid_uuids = [line.strip() for line in f if line.strip()]
        self.valid_uuids = valid_uuids

        self.tracker = AssociationTracker(on_complete_callback=self._on_association_complete)
        self.processor = BackgroundProcessor(
            anonymizer=self.anonymizer,
            db=self.db,
            tracker=self.tracker,
        )

    def open_connection(self, rabbitmq_url):
        self.rabbitmq_url = rabbitmq_url
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        self.connection_rmq = connection
        self.channel = self.connection_rmq.channel()

    def send_heartbeats(self):
        while not self.stop_heartbeat.is_set():
            try:
                if self.connection_rmq and self.connection_rmq.is_open:
                    logging.info("Processing RabbitMQ heartbeat..")
                    self.connection_rmq.process_data_events(time_limit=0)
                    logging.info("Heartbeat processed.")
                else:
                    logging.warning("RabbitMQ connection is closed, attempting to reconnect...")
                    if self.rabbitmq_url:
                        self.open_connection(self.rabbitmq_url)
                        for queue in self.queues:
                            self.channel.queue_declare(queue=queue, passive=False, durable=True)
                        logging.info("Reconnected to RabbitMQ successfully.")
            except Exception as e:
                logging.error(f"Heartbeat error: {e}. Will retry on next cycle.")
            time.sleep(10)

    def close_connection(self):
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
        logging.info(f"Sent message to queues: {self.queues}")

    def send_to_queue_threadsafe(self, message):
        def _publish():
            for q in self.queues:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=q,
                    body=message.encode('utf-8'),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            logging.info(f"Sent message to queues: {self.queues}")

        self.connection_rmq.add_callback_threadsafe(_publish)

    def handle_assoc_open(self, event):
        assoc_id = str(uuid.uuid4())
        ae_title = event.assoc.requestor.ae_title
        ae_address = event.assoc.requestor.address
        ae_port = event.assoc.requestor.port
        event.assoc.assoc_id = assoc_id

        self.tracker.register(assoc_id)

        params = (
            assoc_id,
            ae_title,
            ae_address,
            ae_port,
            datetime.now()
        )
        logging.debug(f"\n{'='*70}")
        logging.debug(f"NEW ASSOCIATION OPENED")
        logging.debug(f"Association ID: {assoc_id}")
        logging.debug(f"Client: {ae_title} ({ae_address}:{ae_port})")
        logging.debug(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.debug(f"{'='*70}")
        self.db.execute_query(INSERT_QUERY_DICOM_ASS, params)

    def handle_assoc_close(self, event):
        assoc_id = getattr(event.assoc, 'assoc_id', None)
        if assoc_id is None:
            logging.warning("Association closed without an assoc_id")
            return

        logging.debug(f"\n{'='*70}")
        logging.debug(f"ASSOCIATION CLOSED")
        logging.debug(f"Association ID: {assoc_id}")
        logging.debug(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.debug(f"{'='*70}")

        self.tracker.mark_closed(assoc_id)

    def handle_store(self, event):
        ds = event.dataset
        ds.file_meta = event.file_meta
        assoc_id = event.assoc.assoc_id

        study_uid = getattr(ds, 'StudyInstanceUID', None)

        if self.valid_uuids:
            if study_uid not in self.valid_uuids:
                logging.error(
                    f"REJECTED: Study UID {study_uid} not in allowed list. "
                    f"Client: {event.assoc.requestor.ae_title}@{event.assoc.requestor.address}"
                )
                return 0xC211

        self.tracker.increment_expected(assoc_id)
        self.processor.enqueue(ds, assoc_id)

        return 0x0000

    def _on_association_complete(self, assoc_id, state):
        logging.info(
            f"Association {assoc_id} complete — "
            f"processed={state.processed_count}, errors={state.error_count}"
        )

        query = """
            SELECT DISTINCT study_instance_uid, patient_id
            FROM dicom_insert
            WHERE assoc_id = %s
        """
        studies = self.db.fetch_all(query, (assoc_id,))

        if studies:
            for study_uid, patient_id in studies:
                try:
                    self.send_to_queue_threadsafe(study_uid)
                    logging.info(f"Queued study {study_uid} for patient {patient_id}")
                except Exception:
                    logging.exception(f"Failed to queue study {study_uid}")

                try:
                    study_folder = os.path.join(BASE_DIR, patient_id, study_uid)
                    self.XNATsender.run(study_folder)
                    logging.info(f"Sent study {study_uid} to XNAT")
                except Exception:
                    logging.exception(f"XNAT upload failed for study {study_uid}")
        else:
            logging.warning(f"Association {assoc_id} completed but no studies found in database")

        if state.error_count == 0 and studies:
            logging.info(
                f"Association {assoc_id} finished successfully — "
                f"{state.processed_count} files, {len(studies)} studies"
            )
        elif state.error_count > 0:
            logging.warning(
                f"Association {assoc_id} finished with {state.error_count} errors"
            )

        gc.collect()
