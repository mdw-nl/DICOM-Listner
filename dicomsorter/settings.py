import os
from pathlib import Path

from config_handler import Config

_mq = Config("rabbitMQ").config

SCP_AE_TITLE = "MY_SCP"
NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10

BASE_DIR = str(Path(__file__).parents[1].resolve() / "data")

PACS_SCP_HOST = os.getenv("PACS_SCP_HOST", "xnat-web")
PACS_SCP_PORT = int(os.getenv("PACS_SCP_PORT", "8104"))
PACS_SCP_AE_TITLE = os.getenv("PACS_SCP_AE_TITLE", "PREACT")
PACS_SCU_AE_TITLE = os.getenv("PACS_SCU_AE_TITLE", "DICOM_SORTER_SCU")
PACS_QUEUE_NAME = os.getenv("PACS_QUEUE_NAME", "pacs_queue")
PACS_CRON_INTERVAL = int(os.getenv("PACS_CRON_INTERVAL", "300"))

USE_RADIOMICS = os.getenv("USE_RADIOMICS", "").strip().lower() in ("1", "true", "yes", "y", "on")
USE_RABBITMQ = os.getenv("USE_RABBITMQ", "true").strip().lower() in ("1", "true", "yes", "y", "on")
USE_PACS = os.getenv("USE_PACS", "true").strip().lower() in ("1", "true", "yes", "y", "on")

QUEUE_NAME = _mq["queue_name"]

if USE_RADIOMICS:
    _radiomics = Config("radiomics").config
    QUEUE_NAME_RADIOMCS = _radiomics["queue_name"]
else:
    QUEUE_NAME_RADIOMCS = None
