import json
import os

from configurations.commons_config import CommonsConfiguration
from configurations.ingestion_config import IngestionConfiguration
from configurations.processing_config import ProcessingConfiguration
from configurations.utility import is_exist
from configurations.utility_configurator import UtilityConfigurator
from commons.logging.simple_logger import logger


class GeneralConfigurator:
    def __init__(self, environment=None, job=None, utility=None):
        self.environment = environment
        self.job = job
        self.utility = utility


class JobConfigurator:
    def __init__(self, ingestion=None, processing=None):
        self.ingestion = IngestionConfiguration(**is_exist(ingestion))
        self.processing = ProcessingConfiguration(**is_exist(processing))


class Configurator:

    def __init__(self):
        self.commons_path = "/config/common_configuration.json"
        self.job_path = "/config/job_configuration.json"
        self.utility_path = "/config/utility_configuration.json"

    def read_config(self, path):
        with open(path, 'r') as f:
            config = json.load(f)
        return config

    def build_config(self, path, cfg):
        if not os.path.exists(path):
            logger.info(f"The file at the specified path '{path}' does not exist")
            return None
        cfg_json = self.read_config(path)
        return cfg(**is_exist(cfg_json))

    def get_configurator(self):
        environment = self.build_config(self.commons_path, CommonsConfiguration) or CommonsConfiguration()
        job = self.build_config(self.job_path, JobConfigurator) or JobConfigurator()
        utility = self.build_config(self.utility_path, UtilityConfigurator) or UtilityConfigurator()
        logger.info("Configuration parameters were passed")

        return GeneralConfigurator(
            environment=environment,
            job=job,
            utility=utility
        )
