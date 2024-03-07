from commons.utils.validators import require_not_none
from commons.logging.simple_logger import logger
from ingestion import constants
from ingestion.data_sources.http_accumulative_data_source import HttpAccumulativeDataSource
from ingestion.data_sources.http_data_source import HttpDataSource
from ingestion.data_sources.sr_http_data_source import SrHttpDataSource


class DataSourceFactory:
    def __init__(self, config, query_runner, query_params_builder,
                 hash_sum_extractor, spark, token_supplier=None, auth=None):
        self.config = config
        self.query_runner = query_runner
        self.query_params_builder = query_params_builder
        self.hash_sum_extractor = hash_sum_extractor
        self.spark = spark
        self.token_supplier = token_supplier
        self.auth = auth

    def create(self):
        data_source = require_not_none(self.config.job.ingestion.data_source.data_source,
                                       f'{self.config.job.ingestion.data_source.data_source} must be set')
        logger.info(f'Using {data_source} source')

        if data_source == constants.GENERIC_REST_DATASOURCE:
            data_source_impl = HttpDataSource(
                self.query_params_builder,
                config=self.config,
                hash_sum_extractor=self.hash_sum_extractor,
                spark=self.spark,
                auth=self.auth
            )
        elif data_source == constants.SMART_RECRUITERS_DATASOURCE:
            data_source_impl = SrHttpDataSource(
                self.query_params_builder,
                config=self.config,
                spark=self.spark,
                auth=self.auth
            )
        elif data_source == constants.ACCUMULATIVE_DATASOURCE:
            data_source_impl = HttpAccumulativeDataSource(
                self.query_params_builder,
                config=self.config,
                spark=self.spark,
                auth=self.auth
            )
        else:
            raise RuntimeError(f'DATA_SOURCE is incorrect: {data_source}')

        return data_source_impl
