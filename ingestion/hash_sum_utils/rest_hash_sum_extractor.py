from commons.logging.simple_logger import logger
from hash_sum_utils.hash_sum_extractor import HashSumExtractor
import json


class RestHashSumExtractor(HashSumExtractor):

    def __init__(self, config):
        self.config = config

    def extract(self, response):
        try:
            json_response = json.loads(response)
            hash_sum = json_response[self.config.job.ingestion.data_manager.hash_sum_response_key]
        except KeyError:
            message = "Response doesn't have hash sum. Check job configs"
            raise Exception(message)
        logger.info(f"Hash sum was received")
        return hash_sum
