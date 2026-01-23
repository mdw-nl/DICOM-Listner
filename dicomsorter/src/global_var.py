import sys
import os

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config_handler import Config

rabbitMQ_config = Config("rabbitMQ").config
user, pwd = rabbitMQ_config["username"], rabbitMQ_config["password"]

SCP_AE_TITLE = "MY_SCP"
NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10
RABBITMQ_URL = f"amqp://{user}:{pwd}@rabbitmq:5672/"
QUEUE_NAME = "DICOM_Processor"
BASE_DIR = os.path.join(PROJECT_ROOT, "data")  # safer absolute path

print(RABBITMQ_URL)