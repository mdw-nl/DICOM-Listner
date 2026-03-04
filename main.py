import logging
import sys
from time import sleep

from pynetdicom import StoragePresentationContexts, debug_logger, evt

from config_handler import Config, load_config_path
from dicomsorter import DicomStoreHandler, PostgresInterface, query
from dicomsorter.src.global_var import NUMBER_ATTEMPTS, RETRY_DELAY_IN_SECONDS, USE_RABBITMQ

BASE_DIR = "dicom_storage"

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
    """Establish connection to the database and check if the required tables exist of if they need to be created
    :return:
    """
    host, port, user, pwd, db = (
        config_dict_db["host"],
        config_dict_db["port"],
        config_dict_db["username"],
        config_dict_db["password"],
        config_dict_db["db"],
    )
    db = PostgresInterface(host=host, database=db, user=user, password=pwd, port=port)
    db.connect()

    # Check if the 'users' table exists
    if db.check_table_exists("dicom_insert"):
        logger.info("The 'dicom_insert' table exists.")
    else:
        logger.info("Table dicom_insert does not exist. Creating....")
        db.execute_query(query=query.CREATE_DATABASE_QUERY)
        logger.info("Table created....")
    db.execute_query(query=query.MIGRATE_ADD_RTSTRUCT_REF)
    db.execute_query(query=query.MIGRATE_ADD_CT_SERIES_REF)

    if db.check_table_exists("associations"):
        logger.info("The 'associations' table exists.")
    else:
        logger.info("Table associations does not exist. Creating....")
        db.execute_query(query=query.CREATE_DATABASE_QUERY_2)
        logger.info("Table created....")
    if db.check_table_exists("calculation_status"):
        logger.info("The 'calculation_status' table exists.")
    else:
        logger.info("Table calculation_status does not exist. Creating....")
        db.execute_query(query=query.CREATE_DATABASE_QUERY_3)
        logger.info("Table created....")
    db.execute_query(query=query.MIGRATE_CALC_STATUS_SOP_UID)
    db.execute_query(query=query.MIGRATE_CALC_STATUS_MODALITY)

    if db.check_table_exists("dvh_result"):
        logger.info("The 'dvh_result' table exists.")
    else:
        logger.info("Table dvh_result does not exist. Creating....")
        db.create_table(
            "dvh_result",
            {
                "result_id": "SERIAL PRIMARY KEY",
                "json_id": "TEXT UNIQUE NOT NULL",  # store your ROI @id
                "dose_bins": "DOUBLE PRECISION[] NOT NULL",
                "volume_bins": "DOUBLE PRECISION[] NOT NULL",
                "D2": "DOUBLE PRECISION",
                "D50": "DOUBLE PRECISION",
                "D95": "DOUBLE PRECISION",
                "D98": "DOUBLE PRECISION",
                "min_dose": "DOUBLE PRECISION",
                "mean_dose": "DOUBLE PRECISION",
                "max_dose": "DOUBLE PRECISION",
                "V0": "DOUBLE PRECISION",
                "V15": "DOUBLE PRECISION",
                "V35": "DOUBLE PRECISION",
            },
        )

        logger.info("Table created....")

    if db.check_table_exists("dvh_package"):
        logger.info("The 'dvh_result' table exists.")
    else:
        logger.info("Table dvh_package does not exist. Creating....")
        db.create_table(
            "dvh_package",
            {
                "sop_instance_uid": "TEXT NOT NULL",
                "roi_name": "TEXT NOT NULL",
                "result_id": "INTEGER NOT NULL REFERENCES dvh_result(result_id) ON DELETE CASCADE",
            },
        )

        logger.info("Table created....")
    return db


# Function to handle incoming DICOM images
if __name__ == "__main__":
    # Set up the DICOM Application Entity (AE) as an SCP

    config_db = Config("postgres").config
    rabbitMQ_config = Config("rabbitMQ").config
    database = set_up_db(config_db)
    path_recipes = load_config_path("recipes")
    dh = DicomStoreHandler(database, path_recipes, send_to_main=True)
    host, port, user, pwd = (
        rabbitMQ_config["host"],
        rabbitMQ_config["port"],
        rabbitMQ_config["username"],
        rabbitMQ_config["password"],
    )

    if USE_RABBITMQ:
        connection_string = f"amqp://{user}:{pwd}@{host}:{port}/"
        for attempt in range(NUMBER_ATTEMPTS):
            logger.info("Trying connection %s for RabbitMQ", attempt)
            try:
                dh.open_connection(connection_string)
                break
            except Exception as e:
                if attempt < NUMBER_ATTEMPTS - 1:
                    logger.info("Retrying in %s seconds...", RETRY_DELAY_IN_SECONDS)
                    sleep(RETRY_DELAY_IN_SECONDS)
                else:
                    raise Exception("Unable to connect to the RabbitMq after time.") from e
        dh.create_queue()
    else:
        logger.info("RabbitMQ disabled (USE_RABBITMQ=false)")
    dh.ae.dimse_timeout = 600  # 10 minutes for slow processing
    dh.ae.network_timeout = 300  # 5 minutes network idle timeout
    dh.ae.supported_contexts = StoragePresentationContexts

    # Define event handlers
    handlers = [
        (evt.EVT_C_STORE, dh.handle_store),
        (evt.EVT_CONN_OPEN, dh.handle_assoc_open),
        (evt.EVT_CONN_CLOSE, dh.handle_assoc_close),
    ]

    logger.info("Starting DICOM Listener on port 104...")
    dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
