import logging
import os
import sys
from time import sleep

import pika
from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE
from pynetdicom.presentation import StoragePresentationContexts

from config_handler import Config
from dicomsorter import PostgresInterface
from dicomsorter.src.global_var import (
    BASE_DIR,
    NUMBER_ATTEMPTS,
    RETRY_DELAY_IN_SECONDS,
    XNAT_QUEUE_NAME,
    XNAT_SCP_AE_TITLE,
    XNAT_SCP_IP,
    XNAT_SCP_PORT,
    XNAT_SCU_AE_TITLE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)


def create_db_connection():
    config_dict_db = Config("postgres").config
    db = PostgresInterface(
        host=config_dict_db["host"],
        database=config_dict_db["db"],
        user=config_dict_db["username"],
        password=config_dict_db["password"],
        port=config_dict_db["port"],
    )
    db.connect()
    return db


def build_rabbitmq_url():
    rabbitmq_config = Config("rabbitMQ").config
    return f"amqp://{rabbitmq_config['username']}:{rabbitmq_config['password']}@{rabbitmq_config['host']}:{rabbitmq_config['port']}/"


def open_rabbitmq_connection(rabbitmq_url: str):
    for attempt in range(NUMBER_ATTEMPTS):
        try:
            logger.info("Connecting to RabbitMQ, attempt %s", attempt + 1)
            return pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        except Exception:
            if attempt == NUMBER_ATTEMPTS - 1:
                raise
            sleep(RETRY_DELAY_IN_SECONDS)


def resolve_study_folder(db, study_uid: str):
    result = db.fetch_one(
        """
        SELECT patient_id
        FROM dicom_insert
        WHERE study_instance_uid = %s
        ORDER BY id ASC
        LIMIT 1
        """,
        (study_uid,),
    )
    if not result:
        return None

    patient_id = result[0]
    return os.path.join(BASE_DIR, patient_id, study_uid)


def iter_dicom_files(folder_path: str):
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.lower().endswith(".dcm"):
                yield os.path.join(root, file_name)


def send_study_to_xnat_scp(study_folder: str) -> int:
    ae = AE(ae_title=XNAT_SCU_AE_TITLE)
    for context in StoragePresentationContexts:
        ae.add_requested_context(context.abstract_syntax, ExplicitVRLittleEndian)

    assoc = ae.associate(XNAT_SCP_IP, XNAT_SCP_PORT, ae_title=XNAT_SCP_AE_TITLE)
    if not assoc.is_established:
        raise ConnectionError(
            f"Failed DICOM association to XNAT SCP {XNAT_SCP_IP}:{XNAT_SCP_PORT} (AE={XNAT_SCP_AE_TITLE})"
        )

    sent = 0
    try:
        for dicom_path in iter_dicom_files(study_folder):
            dataset = dcmread(dicom_path, defer_size="2 MB")
            status = assoc.send_c_store(dataset)
            if not status or getattr(status, "Status", None) not in (0x0000,):
                raise RuntimeError(
                    f"C-STORE failed for {dicom_path}. Status={getattr(status, 'Status', None)}"
                )
            sent += 1
            del dataset
    finally:
        assoc.release()

    if sent == 0:
        logger.warning("No .dcm files found in study folder %s", study_folder)

    return sent


def main():
    db = create_db_connection()
    rabbitmq_url = build_rabbitmq_url()

    while True:
        connection = None
        try:
            connection = open_rabbitmq_connection(rabbitmq_url)
            channel = connection.channel()
            channel.queue_declare(queue=XNAT_QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                study_uid = body.decode("utf-8").strip()
                logger.info("Received study UID for XNAT SCP send: %s", study_uid)
                try:
                    study_folder = resolve_study_folder(db, study_uid)
                    if not study_folder:
                        logger.warning("No database entry found for study %s", study_uid)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    if not os.path.exists(study_folder):
                        logger.warning("Study folder does not exist: %s", study_folder)
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                        return

                    sent = send_study_to_xnat_scp(study_folder)
                    logger.info("Sent %s DICOM files for study %s to XNAT SCP", sent, study_uid)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    logger.exception("Failed XNAT SCP processing for study UID %s", study_uid)
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    except Exception:
                        logger.warning("Could not NACK message, channel likely disconnected.")
                        raise

            channel.basic_consume(queue=XNAT_QUEUE_NAME, on_message_callback=callback)
            logger.info(
                "XNAT worker started. Queue=%s target=%s:%s AE=%s",
                XNAT_QUEUE_NAME,
                XNAT_SCP_IP,
                XNAT_SCP_PORT,
                XNAT_SCP_AE_TITLE,
            )
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("XNAT worker interrupted, shutting down.")
            break
        except Exception:
            logger.exception("XNAT worker lost connection. Reconnecting in %s seconds...", RETRY_DELAY_IN_SECONDS)
            sleep(RETRY_DELAY_IN_SECONDS)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
