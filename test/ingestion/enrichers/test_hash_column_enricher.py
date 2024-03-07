import hashlib
from unittest import TestCase

import chispa
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from commons.configurations.configurator import Configurator
from ingestion.enrichers.hash_column_enricher import HashColumnEnricher


class TestHashColumnEnricher(TestCase):
    spark = SparkSession.builder.getOrCreate()

    def setUp(self) -> None:
        self.context = {}
        self.config = Configurator().get_configurator()

    def test_hash_sum_column(self):
        config = self.config
        config.job.ingestion.general.partition_columns = 'version'
        config.job.ingestion.enrichers.have_hash_sum = True
        config.job.ingestion.enrichers.hash_column = 'hash_sum'

        md5_hash = hashlib.md5()

        df = self.spark.createDataFrame([["test"]])

        # Arrange
        hash_column_enricher = HashColumnEnricher(config)

        # Act
        actual = hash_column_enricher.enrich(df, self.context)

        # Assert
        args = ('test',)
        md5_hash.update(repr(args).encode('utf-8'))
        hash_sum = md5_hash.hexdigest()

        expected = df.withColumn('hash_sum', F.lit(hash_sum))
        chispa.assert_df_equality(actual, expected, ignore_nullable=True)

    def test_no_hash_sum_column(self):
        config = self.config
        config.job.ingestion.enrichers.have_hash_sum = False
        config.job.ingestion.enrichers.hash_column = None
        df = self.spark.createDataFrame([["test"]])

        # Arrange
        hash_column_enricher = HashColumnEnricher(config)

        # Act
        actual = hash_column_enricher.enrich(df, self.context)

        # Assert
        self.assertEqual(actual, df)
