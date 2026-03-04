import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def load_config_path(folder, file_name=None):
    base = Path(__file__).parent
    config_path = base / folder if file_name is None else base / folder / file_name
    config_path = config_path.resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    return str(config_path)


def read_config(folder, file_name):
    config_path = (Path(__file__).parent / folder / file_name).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with config_path.open() as file:
        return yaml.safe_load(file)


class Config:
    def __init__(self, section_name):
        file_data = read_config("Config", "config.yaml")
        self.config: dict = {}
        self.read_config_section(file_data, section_name)

    def read_config_section(self, file_data, sect):
        self.config = file_data.get(sect, {})
        logger.info("Config data: %s", self.config)
