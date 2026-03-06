import logging
import sys

from pynetdicom import StoragePresentationContexts, debug_logger, evt

from config_handler import Config, load_config_path
from dicomsorter import DicomStoreHandler, PostgresInterface, queries
from dicomsorter.queue import MessageQueue
from dicomsorter.settings import (
    PACS_CRON_INTERVAL,
    PACS_QUEUE_NAME,
    QUEUE_NAME,
    QUEUE_NAME_RADIOMCS,
    USE_PACS,
    USE_RABBITMQ,
    USE_RADIOMICS,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger()

debug_logger()
logging.getLogger("pynetdicom").setLevel(logging.DEBUG)


def set_up_db(config_dict_db):
    host, port, user, pwd, db_name = (
        config_dict_db["host"],
        config_dict_db["port"],
        config_dict_db["username"],
        config_dict_db["password"],
        config_dict_db["db"],
    )
    db = PostgresInterface(host=host, database=db_name, user=user, password=pwd, port=port)
    db.connect()

    for _, ddl in queries.TABLES:
        db.execute_query(ddl)

    for migration in queries.MIGRATIONS:
        db.execute_query(migration)

    return db


if __name__ == "__main__":
    config_db = Config("postgres").config
    rabbitMQ_config = Config("rabbitMQ").config
    database = set_up_db(config_db)
    path_recipes = load_config_path("recipes")

    mq = None
    if USE_RABBITMQ:
        queues = [QUEUE_NAME]
        if USE_RADIOMICS and QUEUE_NAME_RADIOMCS:
            queues.append(QUEUE_NAME_RADIOMCS)
        if USE_PACS:
            queues.append(PACS_QUEUE_NAME)
        mq = MessageQueue(queues)
        host, port, user, pwd = (
            rabbitMQ_config["host"],
            rabbitMQ_config["port"],
            rabbitMQ_config["username"],
            rabbitMQ_config["password"],
        )
        mq.connect(f"amqp://{user}:{pwd}@{host}:{port}/")
        mq.declare_queues()
        mq.start_heartbeat()
    else:
        logger.info("RabbitMQ disabled (USE_RABBITMQ=false)")

    if USE_PACS and USE_RABBITMQ:
        from dicomsorter.pacs import DICOMtoPACS, PacsConsumer

        mq_url = f"amqp://{rabbitMQ_config['username']}:{rabbitMQ_config['password']}@{rabbitMQ_config['host']}:{rabbitMQ_config['port']}/"
        pacs_consumer = PacsConsumer(database, mq_url, DICOMtoPACS(), PACS_CRON_INTERVAL)
        pacs_consumer.start()
        logger.info("PACS consumer started (interval=%ss)", PACS_CRON_INTERVAL)
    elif USE_PACS:
        logger.warning("USE_PACS=true but USE_RABBITMQ=false — PACS archiving disabled")

    dh = DicomStoreHandler(database, path_recipes, mq=mq)
    dh.ae.dimse_timeout = 600
    dh.ae.network_timeout = 300
    dh.ae.supported_contexts = StoragePresentationContexts

    handlers = [
        (evt.EVT_C_STORE, dh.handle_store),
        (evt.EVT_CONN_OPEN, dh.handle_assoc_open),
        (evt.EVT_CONN_CLOSE, dh.handle_assoc_close),
    ]

    logger.info("Starting DICOM Listener on port 104...")
    dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
