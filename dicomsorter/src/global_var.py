from config_handler import Config

rabbitMQ_config = Config("rabbitMQ").config
user, pwd = rabbitMQ_config["username"], rabbitMQ_config["password"]

SCP_AE_TITLE = "MY_SCP"
NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10
RABBITMQ_URL = f"amqp://{user}:{pwd}@rabbitmq:5672/"
QUEUE_NAME = "DICOM_Processor"
BASE_DIR = "./data/"
