import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2].resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config_handler import Config  # noqa: E402

rabbitMQ_config = Config("rabbitMQ").config

user, pwd = rabbitMQ_config["username"], rabbitMQ_config["password"]

SCP_AE_TITLE = "MY_SCP"
NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10
RABBITMQ_URL = f"amqp://{user}:{pwd}@rabbitmq:5672/"
QUEUE_NAME = rabbitMQ_config["queue_name"]

BASE_DIR = str(PROJECT_ROOT / "data")
ELASTICSEARCH_URL = "http://localhost:9200"
XNAT_USERNAME = "admin"
USE_RADIOMICS = os.getenv("USE_RADIOMICS", "").strip().lower() in ("1", "true", "yes", "y", "on")
USE_RABBITMQ = os.getenv("USE_RABBITMQ", "true").strip().lower() in ("1", "true", "yes", "y", "on")
USE_XNAT = os.getenv("USE_XNAT", "true").strip().lower() in ("1", "true", "yes", "y", "on")
XNAT_PASSWORD = "admin"
XNAT_URL = "http://xnat-nginx:80"
XNAT_SCP_HOST = os.getenv("XNAT_SCP_HOST", "xnat-web")
XNAT_SCP_PORT = int(os.getenv("XNAT_SCP_PORT", "8104"))
XNAT_SCP_AE_TITLE = os.getenv("XNAT_SCP_AE_TITLE", "PREACT")
if USE_RADIOMICS:
    radiomics_config = Config("radiomics").config
    QUEUE_NAME_RADIOMCS = radiomics_config["queue_name"]
else:
    QUEUE_NAME_RADIOMCS = None
