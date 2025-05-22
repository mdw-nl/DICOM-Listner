from pydicom import dcmread
import logging
from pynetdicom import evt, StoragePresentationContexts, debug_logger
from dicomsorter import PostgresInterface, DicomStoreHandler, query
from dicomsorter.src.global_var import NUMBER_ATTEMPTS, RETRY_DELAY_IN_SECONDS
from time import sleep
import yaml

BASE_DIR = "dicom_storage"

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


class Config:
    def __init__(self, section_name):
        file = read_config()
        self.config = None
        self.read_config_section(file, section_name)

    def read_config_section(self, file, sect):
        self.config = file.get(sect, {})
        logging.info(f"Config data : {self.config}")


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
    return db


# Function to handle incoming DICOM images
if __name__ == "__main__":
    # Set up the DICOM Application Entity (AE) as an SCP
    config_db = Config("postgres").config
    rabbitMQ_config = Config("rabbitMQ").config

    database = set_up_db(config_db)
    dh = DicomStoreHandler(database)
    host, port, user, pwd = rabbitMQ_config["host"], rabbitMQ_config["port"] \
        , rabbitMQ_config["username"], rabbitMQ_config["password"]

    connection_string = f"amqp://{user}:{pwd}@{host}:{port}/"
    for attempt in range(NUMBER_ATTEMPTS):
        logging.info(f"Trying connection {attempt} for RabbitMQ")
        try:
            dh.open_connection(connection_string)
        except:
            if attempt < NUMBER_ATTEMPTS - 1:
                logging.info(f"Retrying in {RETRY_DELAY_IN_SECONDS} seconds...")
                sleep(RETRY_DELAY_IN_SECONDS)
            else:
                raise Exception(
                    f"Unable to connect to the RabbitMq after time.")
    dh.create_queue()
    dh.ae.supported_contexts = StoragePresentationContexts

    # Define event handlers
    handlers = [(evt.EVT_C_STORE, dh.handle_store),
                (evt.EVT_CONN_OPEN, dh.handle_assoc_open), (evt.EVT_CONN_CLOSE, dh.handle_assoc_close)]

    print("[INFO] Starting DICOM Listener on port 104...")
    dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
