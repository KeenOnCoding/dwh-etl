from abc import ABC, abstractmethod
from pyspark.sql.functions import explode

from commons.logging.simple_logger import logger
from commons.spark.spark_utils import get_df_data_size, create_empty_dataframe
from ingestion.hash_sum_utils.hash_sum_manager import HashSumManager


class AbstractDataSource(ABC):
    def extract_data(self, batch_context, **kwargs):
        raw_data = self._extract_data_from_source_system(batch_context, **kwargs)
        logger.info("Data was extracted")
        return self._process_response(raw_data, batch_context, **kwargs)

    def _process_response(self, response, batch_context, **kwargs):
        self._pre_process(response, batch_context, **kwargs)
        response_df = self._read_data(response)
        response_df = self._post_process(response_df)
        logger.info("Creating dataframe from response")
        return response_df

    def _pre_process(self, response, batch_context, **kwargs):
        pass

    def _post_process(self, dataframe):
        return dataframe

    def nested_data_explode(self, spark, dataframe, response_key):
        response_size = get_df_data_size(dataframe, response_key)
        if response_size > 0:
            dataframe = dataframe.select(
                explode(response_key).alias("data")
            ).select("data.*")
        else:
            dataframe = create_empty_dataframe(spark)
        return dataframe

    def save_data_hashsum(self, response, batch_context,
                          hash_sum_extractor, **kwargs):
        try:
            from ingestion.constants import HASH_SUM_MANAGER
            hash_sum_manager: HashSumManager = batch_context[HASH_SUM_MANAGER]
            logger.info(f"Using HashSumManager: '{HASH_SUM_MANAGER}'")
        except KeyError:
            raise RuntimeError('TRACK_HASH_SUM is active but no '
                               'hashsum manager found in **kwargs')

        hash_sum = hash_sum_extractor.extract(response.text)
        batch_id = batch_context.get_batch_id()
        logger.info(f"Batch id: {batch_id}. Hash sum: {hash_sum}")

        hash_sum_manager.save_one(hash_sum, batch_id,
                                  self.get_raw_parameters(
                                      batch_context, **kwargs))

    def get_raw_parameters(self, batch_context=None, **kwargs):
        pass

    def read_json(self, data, spark):
        return spark.read.json(data, multiLine=True, primitivesAsString=True)

    @abstractmethod
    def _read_data(self, response):
        pass
