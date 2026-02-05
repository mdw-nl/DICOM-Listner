import yaml
import logging
import os


def load_config_path(folder, file_name=None):
    base = os.path.dirname(__file__)
    if file_name is None:
        config_path = os.path.join(base, folder)
    else:
        config_path = os.path.join(base, folder, file_name)
    config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    return config_path


def read_config(folder, file_name):
    config_path = os.path.join(os.path.dirname(__file__), folder, file_name)
    config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


class Config:
    def __init__(self, section_name):
        file_data = read_config('Config', 'config.yaml')
        self.config = None
        self.read_config_section(file_data, section_name)

    def read_config_section(self, file_data, sect):
        self.config = file_data.get(sect, {})
        logging.info(f"Config data: {self.config}")
