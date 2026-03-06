import sys
import os

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config_handler import Config

rabbitMQ_config = Config("rabbitMQ").config
xnat_config = Config("Xnat").config


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

user, pwd = rabbitMQ_config["username"], rabbitMQ_config["password"]

SCP_AE_TITLE = "MY_SCP"
NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10
RABBITMQ_URL = f"amqp://{user}:{pwd}@rabbitmq:5672/"
QUEUE_NAME = rabbitMQ_config["queue_name"]
ANONYMIZER_QUEUE_NAME = rabbitMQ_config.get("anonymizer_queue_name", "DICOM_Anonymizer")
XNAT_QUEUE_NAME = rabbitMQ_config.get("xnat_queue_name", "DICOM_XNAT")
USE_ANONYMIZER = _as_bool(os.getenv("USE_ANONYMIZER"), _as_bool(rabbitMQ_config.get("use_anonymizer", True), True))
ANONYMIZER_PUBLISH_TO_QUEUE_NAME = _as_bool(
    os.getenv("ANONYMIZER_PUBLISH_TO_QUEUE_NAME"),
    _as_bool(rabbitMQ_config.get("anonymizer_publish_to_queue_name", True), True),
)

BASE_DIR = os.path.join(PROJECT_ROOT, "data")  # safer absolute path
ANONYMIZED_BASE_DIR = os.path.join(PROJECT_ROOT, "anonymized_data")
ELASTICSEARCH_URL = "http://localhost:9200"
XNAT_SCU_AE_TITLE = xnat_config.get("scu_ae_title", "DICOM_SORTER_SCU")
XNAT_SCP_AE_TITLE = xnat_config["ae_title"]
XNAT_SCP_IP = xnat_config["ip"]
XNAT_SCP_PORT = int(xnat_config["port"])

USE_RADIOMICS = os.getenv("USE_RADIOMICS", "").strip().lower() in ("1", "true", "yes", "y", "on")
if USE_RADIOMICS:
    radiomics_config = Config("radiomics").config
    QUEUE_NAME_RADIOMCS = radiomics_config["queue_name"]
else:
    QUEUE_NAME_RADIOMCS = None
