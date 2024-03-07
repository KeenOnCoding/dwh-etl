from pyspark.sql.types import StructType

from ingestion.data_sources.http_data_source import HttpDataSource
from commons.logging.simple_logger import logger


class HttpAccumulativeDataSource(HttpDataSource):

    def __init__(self, query_params_builder, config, spark, auth):
        HttpDataSource.__init__(self,  query_params_builder)
        self.config = config
        self.spark = spark
        self.auth = auth

    def extract_data(self, batch_context, **kwargs):
        accumulator = self._init_accumulator()
        predicate = self._generate_predicate()
        while predicate(accumulator):
            data = super(HttpDataSource, self).extract_data(batch_context,
                                                            **kwargs)
            accumulator = self._accumulate(accumulator, data)

        return accumulator

    def _generate_predicate(self):
        """Predicate must consume acumulator and return the boolean
        of its state"""
        __slots__ = ('config', 'spark')

        def _make_predicate(times):
            counter = 0

            # use inner function to preserve state between calls
            # after the method is called, the counter is incremented
            def predict_true_n_times(*_, **__):
                nonlocal counter
                if counter != times:
                    counter += 1
                    return True
                return False

            return predict_true_n_times

        return _make_predicate(self.config.job.ingestion.data_source.limiter)

    def _init_accumulator(self):
        return self.spark.createDataFrame([], StructType([]))

    def _accumulate(self, accumulator, data):
        if accumulator.count() == 0:
            accumulator = self.spark.createDataFrame([], schema=data.schema)
        return accumulator.union(data)

    def _read_data(self, response):
        sc = self.spark.sparkContext
        parsed_response = sc.parallelize([response.text])
        return super().read_json(parsed_response, self.spark)

    def _post_process(self, response_df):
        if self.config.job.ingestion.data_source.data_response_key is not None:
            response_df = super().nested_data_explode(
                self.spark, response_df,
                self.config.job.ingestion.data_source.data_response_key
            )
        logger.info("Creating dataframe from response")
        return response_df
