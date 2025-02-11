from pydicom import dcmread
import logging
from pynetdicom import evt, StoragePresentationContexts, debug_logger
from dicomsorter import PostgresInterface, DicomStoreHandler, query

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
    return db

# Function to handle incoming DICOM images
if __name__ == "__main__":
    # Set up the DICOM Application Entity (AE) as an SCP

    database = set_up_db()
    dh = DicomStoreHandler(database)
    # Accept all DICOM storage types, without this we need to specify this manually

    dh.ae.supported_contexts = StoragePresentationContexts

    # Define event handlers
    handlers = [(evt.EVT_C_STORE, dh.handle_store)]

    print("[INFO] Starting DICOM Listener on port 104...")
    dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
