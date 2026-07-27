import logging
from pydicom.uid import ImplicitVRLittleEndian, ExplicitVRLittleEndian, ExplicitVRBigEndian
from pynetdicom import evt, StoragePresentationContexts, debug_logger
from pynetdicom.sop_class import register_uid
from pynetdicom.service_class import StorageServiceClass
from dicomsorter import PostgresInterface, DicomStoreHandler, query
import traceback
import yaml
from dicomsorter.config_handler import Config

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

debug_logger()
logging.getLogger("pynetdicom").setLevel(logging.DEBUG)




def read_config():
    with open('Config/config.yaml', 'r') as file:
        file_red = yaml.safe_load(file)
        return file_red



def set_up_db(config_dict_db):
    """
    Establish connection to the database and check if the required tables exist of if they need to be created
    :return:
    """
    host, port, user, pwd, db = config_dict_db["host"], config_dict_db["port"], \
        config_dict_db["username"], config_dict_db["password"], config_dict_db["db"]
    db = PostgresInterface(host=host, database=db, user=user, password=pwd, port=port)
    db.connect()

    # Check if the 'users' table exists
    if db.check_table_exists("dicom_insert"):
        logger.info("The 'dicom_insert' table exists.")
    else:
        logger.info("Table dicom_insert does not exist. Creating....")
        db.execute_query(query=query.CREATE_DATABASE_QUERY)
        logger.info("Table created....")

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
    if db.check_table_exists("dvh_results"):
        logger.info("The 'dvh_results' table exists.")
    else:
        logger.info("Table dvh_results does not exist. Creating....")
        db.execute_query(query=query.CREATE_DATABASE_QUERY_DVH)
        logger.info("Table created....")
    return db


# Function to handle incoming DICOM images
if __name__ == "__main__":
    # Set up the DICOM Application Entity (AE) as an SCP
    config_db = Config("postgres").config
    rabbitMQ_config = Config("rabbitMQ").config
    private_sop_config = Config("private_sop_classes").config

    # Keep only well-formed entries; warn about (and skip) anything invalid.
    valid_sop_classes = []
    if private_sop_config and not isinstance(private_sop_config, list):
        logger.warning(
            f"'private_sop_classes' must be a list of entries, got {type(private_sop_config).__name__}; ignoring."
        )
    elif private_sop_config:
        for sop_class in private_sop_config:
            if not isinstance(sop_class, dict):
                logger.warning(f"Skipping non-mapping private_sop_classes entry: {sop_class!r}")
                continue
            uid = sop_class.get("uid")
            keyword = sop_class.get("keyword")
            if not uid or not keyword:
                logger.warning(
                    f"Skipping malformed private_sop_classes entry (needs 'uid' and 'keyword'): {sop_class}"
                )
                continue
            try:
                register_uid(uid, keyword, StorageServiceClass)
            except Exception as e:
                logger.warning(f"Skipping private_sop_classes entry {sop_class}: register_uid failed ({e})")
                continue
            valid_sop_classes.append(sop_class)

    database = set_up_db(config_db)
    dh = DicomStoreHandler(database)
    host, port, user, pwd = rabbitMQ_config["host"], rabbitMQ_config["port"] \
        , rabbitMQ_config["username"], rabbitMQ_config["password"]

    connection_string = f"amqp://{user}:{pwd}@{host}:{port}/"

    try:
        dh.attempt_connection_rmq(connection_string)
    except Exception as e:
        logger.error(f"An error occurred while trying to connect to rabbitmq: {e}")
        logger.error(traceback.format_exc())
        raise e

    try:
        dh.create_queue()
        dh.ae.supported_contexts = StoragePresentationContexts
        # Accept the private / non-standard Storage SOP classes listed in config.
        for sop_class in valid_sop_classes:
            dh.ae.add_supported_context(
                sop_class["uid"],
                [ImplicitVRLittleEndian, ExplicitVRLittleEndian, ExplicitVRBigEndian],
            )

        # Define event handlers
        handlers = [(evt.EVT_C_STORE, dh.handle_store),
                    (evt.EVT_CONN_OPEN, dh.handle_assoc_open), (evt.EVT_CONN_CLOSE, dh.handle_assoc_close)]

        logger.info("[INFO] Starting DICOM Listener on port 104...")
        dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.error(traceback.format_exc())
        raise e
