import gc
import logging
import os
import sys
from time import sleep

import pika
from pydicom import dcmread

from anonymization import Anonymizer
from config_handler import Config, load_config_path
from dicomsorter import PostgresInterface
from dicomsorter.src.global_var import (
    ANONYMIZER_QUEUE_NAME,
    NUMBER_ATTEMPTS,
    QUEUE_NAME,
    RETRY_DELAY_IN_SECONDS,
    USE_ANONYMIZER,
    USE_XNAT_QUEUE,
    XNAT_QUEUE_NAME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

DICOM_BATCH_SIZE = int(os.getenv("ANONYMIZER_DICOM_BATCH_SIZE", "100"))


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


def build_patient_mapping_for_study(db, study_uid: str):
    """
    Build a minimal runtime map per study to avoid unbounded memory growth.

    Mapping is DB-driven (patient_id_map) and also includes identity mappings for
    IDs already present in dicom_insert so re-processing anonymized files remains
    idempotent.
    """
    patient_rows = db.fetch_all(
        """
        SELECT DISTINCT patient_id
        FROM dicom_insert
        WHERE study_instance_uid = %s
          AND patient_id IS NOT NULL
        """,
        (study_uid,),
    )

    mapping = {}
    if not patient_rows:
        return mapping

    for (patient_id,) in patient_rows:
        if not patient_id:
            continue

        mapping[patient_id] = patient_id
        map_rows = db.fetch_all(
            """
            SELECT original_patient_id, generated_patient_id
            FROM patient_id_map
            WHERE original_patient_id = %s
               OR generated_patient_id = %s
            """,
            (patient_id, patient_id),
        )

        if not map_rows:
            continue

        for original_patient_id, generated_patient_id in map_rows:
            mapping[original_patient_id] = generated_patient_id
            mapping[generated_patient_id] = generated_patient_id

    return mapping


def iter_study_file_paths(db, study_uid: str, batch_size: int = DICOM_BATCH_SIZE):
    last_seen_id = 0
    while True:
        rows = db.fetch_all(
            """
            SELECT id, file_path
            FROM dicom_insert
            WHERE study_instance_uid = %s
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (study_uid, last_seen_id, batch_size),
        )

        if not rows:
            break

        for row_id, dicom_path in rows:
            last_seen_id = row_id
            yield dicom_path


def anonymize_study(db, anonymizer: Anonymizer, study_uid: str) -> int:
    processed = 0

    for dicom_path in iter_study_file_paths(db, study_uid):
        if not dicom_path or not os.path.exists(dicom_path):
            logger.warning("Skipping missing file path: %s", dicom_path)
            continue

        dataset = None
        anonymized_ds = None
        try:
            dataset = dcmread(dicom_path, defer_size="1 MB")
            anonymized_ds = anonymizer.run(dataset)
            if anonymized_ds is None:
                raise RuntimeError(f"Anonymization failed for {dicom_path}")

            anonymized_ds.save_as(dicom_path, enforce_file_format=True)
            processed += 1
        finally:
            del dataset
            del anonymized_ds
            gc.collect()

    if processed == 0:
        logger.warning("No DICOM rows found for study UID %s", study_uid)
    else:
        logger.info("Anonymized %s files for study UID %s", processed, study_uid)

    return processed


def main():
    if not USE_ANONYMIZER:
        logger.warning("USE_ANONYMIZER is disabled. This worker should not be running.")

    db = create_db_connection()
    recipes_path = load_config_path("recipes")
    anonymizer = Anonymizer(path_files=recipes_path, patient_map_override={}, use_csv_lookup=False)
    rabbitmq_url = build_rabbitmq_url()
    logger.info("USE_XNAT_QUEUE=%s", USE_XNAT_QUEUE)

    while True:
        connection = None
        try:
            connection = open_rabbitmq_connection(rabbitmq_url)
            channel = connection.channel()
            channel.queue_declare(queue=ANONYMIZER_QUEUE_NAME, durable=True)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            if USE_XNAT_QUEUE:
                channel.queue_declare(queue=XNAT_QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                study_uid = body.decode("utf-8").strip()
                logger.info("Received study UID for anonymization: %s", study_uid)
                try:
                    anonymizer._patient_map = build_patient_mapping_for_study(db, study_uid)

                    processed = anonymize_study(db, anonymizer, study_uid)
                    if processed > 0:
                        ch.basic_publish(
                            exchange="",
                            routing_key=QUEUE_NAME,
                            body=study_uid.encode("utf-8"),
                            properties=pika.BasicProperties(delivery_mode=2),
                        )
                        if USE_XNAT_QUEUE:
                            ch.basic_publish(
                                exchange="",
                                routing_key=XNAT_QUEUE_NAME,
                                body=study_uid.encode("utf-8"),
                                properties=pika.BasicProperties(delivery_mode=2),
                            )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    logger.exception("Failed processing study UID %s", study_uid)
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    except Exception:
                        logger.warning("Could not NACK message, channel likely disconnected.")
                        raise

            channel.basic_consume(queue=ANONYMIZER_QUEUE_NAME, on_message_callback=callback)
            logger.info("Anonymizer worker started. Queue=%s", ANONYMIZER_QUEUE_NAME)
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Anonymizer worker interrupted, shutting down.")
            break
        except Exception:
            logger.exception("Anonymizer worker lost connection. Reconnecting in %s seconds...", RETRY_DELAY_IN_SECONDS)
            sleep(RETRY_DELAY_IN_SECONDS)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
