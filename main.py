from pydicom import dcmread
import logging
from pynetdicom import evt, StoragePresentationContexts, debug_logger
from dicomsorter import PostgresInterface, DicomStoreHandler, query
from dicomsorter.src.global_var import NUMBER_ATTEMPTS, RETRY_DELAY_IN_SECONDS
from time import sleep
BASE_DIR = "dicom_storage"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

debug_logger()
logging.getLogger("pynetdicom").setLevel(logging.DEBUG)


def set_up_db():
    """
    Establish connection to the database and check if the required tables exist of if they need to be created
    :return:
    """
    db = PostgresInterface(host="postgres", database="postgres", user="postgres", password="postgres", port=5432)
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
    return db


# Function to handle incoming DICOM images
if __name__ == "__main__":
    # Set up the DICOM Application Entity (AE) as an SCP

    database = set_up_db()
    dh = DicomStoreHandler(database)
    for attempt in range(NUMBER_ATTEMPTS):
        logging.info(f"Trying connection {attempt} for RabbitMQ")
        try:
            dh.open_connection()
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
                (evt.EVT_CONN_OPEN, dh.handle_assoc_open), (evt.EVT_CONN_CLOSE,dh.handle_assoc_close)]

    print("[INFO] Starting DICOM Listener on port 104...")
    dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
