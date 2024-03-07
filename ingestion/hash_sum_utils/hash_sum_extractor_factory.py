from hash_sum_utils.rest_hash_sum_extractor \
    import RestHashSumExtractor
from commons.logging.simple_logger import logger


class HashSumExtractorFactory:

    def __init__(self, config):
        self.config = config

    def create(self):
        extractor_strategy = self.config.job.ingestion.data_manager.hash_sum_extractor_impl
        logger.info(f"Using {extractor_strategy} hash sum extractor strategy")

        if extractor_strategy == 'REST':
            extractor = RestHashSumExtractor(self.config)
        elif extractor_strategy is None:
            logger.info(f"Using default REST strategy")
            extractor = RestHashSumExtractor(self.config)
        else:
            raise Exception("HASH_SUM_EXTRACTOR_IMPL is not correct")

        return extractor
