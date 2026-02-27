import logging
import os
import sys
from time import sleep

import pika

from config_handler import Config, load_config_path
from dicomsorter import PostgresInterface
from dicomsorter.XNAThandler import DICOMtoXNAT
from dicomsorter.src.global_var import (
    BASE_DIR,
    NUMBER_ATTEMPTS,
    RETRY_DELAY_IN_SECONDS,
    XNAT_QUEUE_NAME,
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


def main():
    db = create_db_connection()
    treat_file = os.path.join(load_config_path("recipes"), "treatment.csv")
    xnat_sender = DICOMtoXNAT(treatment_path=treat_file)

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
    channel.queue_declare(queue=XNAT_QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        study_uid = body.decode("utf-8").strip()
        logger.info("Received study UID for XNAT upload: %s", study_uid)
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

            xnat_sender.run(study_folder)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logger.exception("Failed XNAT processing for study UID %s", study_uid)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=XNAT_QUEUE_NAME, on_message_callback=callback)
    logger.info("XNAT worker started. Queue=%s", XNAT_QUEUE_NAME)
    channel.start_consuming()


if __name__ == "__main__":
    main()
