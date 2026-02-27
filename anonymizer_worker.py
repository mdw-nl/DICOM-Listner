import logging
import os
import sys

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
    XNAT_QUEUE_NAME,
)
from time import sleep

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



def load_patient_mapping(db):
    rows = db.fetch_all(
        """
        SELECT original_patient_id, generated_patient_id
        FROM patient_id_map
        """
    )
    if not rows:
        return {}
    return {row[0]: row[1] for row in rows}

def anonymize_study(db, anonymizer: Anonymizer, study_uid: str) -> int:
    rows = db.fetch_all(
        """
        SELECT file_path
        FROM dicom_insert
        WHERE study_instance_uid = %s
        """,
        (study_uid,),
    )

    if not rows:
        logger.warning("No DICOM rows found for study UID %s", study_uid)
        return 0

    processed = 0
    for (dicom_path,) in rows:
        if not dicom_path or not os.path.exists(dicom_path):
            logger.warning("Skipping missing file path: %s", dicom_path)
            continue

        dataset = dcmread(dicom_path)
        anonymized_ds = anonymizer.run(dataset)
        if anonymized_ds is None:
            raise RuntimeError(f"Anonymization failed for {dicom_path}")

        anonymized_ds.save_as(dicom_path, write_like_original=False)
        processed += 1

    logger.info("Anonymized %s files for study UID %s", processed, study_uid)
    return processed


def main():
    db = create_db_connection()
    recipes_path = load_config_path("recipes")
    patient_map = load_patient_mapping(db)
    anonymizer = Anonymizer(path_files=recipes_path, patient_map_override=patient_map, use_csv_lookup=False)

    connection = None
    rabbitmq_url = build_rabbitmq_url()
    for attempt in range(NUMBER_ATTEMPTS):
        try:
            logger.info("Connecting to RabbitMQ, attempt %s", attempt + 1)
            connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            break
        except Exception:
            if attempt == NUMBER_ATTEMPTS - 1:
                raise
            sleep(RETRY_DELAY_IN_SECONDS)

    channel = connection.channel()
    channel.queue_declare(queue=ANONYMIZER_QUEUE_NAME, durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=XNAT_QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    if not USE_ANONYMIZER:
        logger.warning("USE_ANONYMIZER is disabled. This worker should not be running.")

    if ANONYMIZER_QUEUE_NAME == QUEUE_NAME:
        raise ValueError("anonymizer_queue_name must be different from queue_name to avoid queue loops")

    def callback(ch, method, properties, body):
        study_uid = body.decode("utf-8").strip()
        logger.info("Received study UID for anonymization: %s", study_uid)
        try:
            anonymizer._patient_map.update(load_patient_mapping(db))
            processed = anonymize_study(db, anonymizer, study_uid)
            if processed > 0:
                ch.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=study_uid.encode("utf-8"),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                ch.basic_publish(
                    exchange="",
                    routing_key=XNAT_QUEUE_NAME,
                    body=study_uid.encode("utf-8"),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logger.exception("Failed processing study UID %s", study_uid)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=ANONYMIZER_QUEUE_NAME, on_message_callback=callback)
    logger.info("Anonymizer worker started. Queue=%s", ANONYMIZER_QUEUE_NAME)
    channel.start_consuming()


if __name__ == "__main__":
    main()
