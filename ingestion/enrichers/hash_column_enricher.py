import hashlib

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from commons.logging.simple_logger import logger
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class HashColumnEnricher(AbstractEnricher):
    """
    HashColumnEnricher is used to create column with MD5 hash value of each row
    """

    def __init__(self, config):
        self.hash_column_name = config.job.ingestion.enrichers.hash_column
        self.have_hash_sum = config.job.ingestion.enrichers.have_hash_sum

    def __get_hash_sum(self, values_list):
        values_tuple = tuple(values_list)
        string_repr = repr(values_tuple).encode('utf-8')

        hash_sum = hashlib.md5()
        hash_sum.update(string_repr)

        return hash_sum.hexdigest()

    def enrich(self, data: DataFrame, context):
        get_hash_sum = F.udf(self.__get_hash_sum, StringType())

        if self.have_hash_sum:
            logger.info("Using HashColumnEnricher: creating hash row for each row")
            columns_list = F.array([data[col] for col in data.columns])
            data = data.withColumn(self.hash_column_name,
                                   get_hash_sum(columns_list))

        return data
