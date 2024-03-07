import datetime
import unittest
from unittest import TestCase

import chispa
import pyspark.sql.functions as F

from processor.daily_processor.main import split_data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, BooleanType, IntegerType


class TestSplitData(TestCase):
    spark = SparkSession.builder.appName("daily_processor").getOrCreate()

    def setUp(self):
        self.daily_split = split_data
        self.schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('effort', DoubleType(), True),
            StructField('overtime', BooleanType(), True),
            StructField('started', TimestampType(), True),
            StructField('finished', TimestampType(), True),
        ])

    def test_split_data(self):
        # Arrange
        data = [(1, 24.0, False,
                 datetime.datetime(2013, 5, 20, 00, 00, 00),
                 datetime.datetime(2013, 5, 21, 00, 00, 00),
                 )]

        schema = self.schema
        df = self.spark.createDataFrame(data=data, schema=schema)

        # Act
        actual = self.daily_split(df, [])

        # Assert
        expected = self.spark.createDataFrame(
            [(1, 12.0, False,
              datetime.datetime(2013, 5, 20, 00, 00, 00),
              datetime.datetime(2013, 5, 20, 00, 00, 00),),
             (1, 12.0, False,
              datetime.datetime(2013, 5, 21, 00, 00, 00),
              datetime.datetime(2013, 5, 21, 00, 00, 00),
              )],
            schema) \
            .withColumn("started_date", F.col("started").cast("date")) \
            .withColumn("finished_date", F.col("finished").cast("date"))

        chispa.assert_df_equality(actual, expected)

    def test_one_day_split_data(self):
        # Arrange
        data = [(1, 24.0, False, datetime.datetime(2013, 5, 20, 00, 00, 00),
                 datetime.datetime(2013, 5, 20, 00, 00, 00))]
        schema = self.schema
        df = self.spark.createDataFrame(data=data, schema=schema)

        # Act
        actual = self.daily_split(df, [])

        # Assert
        expected = self.spark.createDataFrame(
            [(1, 24.0, False, datetime.datetime(2013, 5, 20, 00, 00, 00),
              datetime.datetime(2013, 5, 20, 00, 00, 00))],
            schema) \
            .withColumn("started_date", F.col("started").cast("date")) \
            .withColumn("finished_date", F.col("finished").cast("date"))

        chispa.assert_df_equality(actual, expected)

    def test_weekend_split_data(self):
        # Arrange
        data = [(1, 24.0, False, datetime.datetime(2021, 9, 10, 00, 00, 00),
                 datetime.datetime(2021, 9, 13, 00, 00, 00))]
        schema = self.schema

        df = self.spark.createDataFrame(data=data, schema=schema)

        # Act
        actual = self.daily_split(df, [])

        # Assert
        expected = self.spark.createDataFrame(
            [(1, 12.0, False, datetime.datetime(2021, 9, 10, 00, 00, 00),
              datetime.datetime(2021, 9, 10, 00, 00, 00)),
             (1, 12.0, False, datetime.datetime(2021, 9, 13, 00, 00, 00),
              datetime.datetime(2021, 9, 13, 00, 00, 00))],
            schema) \
            .withColumn("started_date", F.col("started").cast("date")) \
            .withColumn("finished_date", F.col("finished").cast("date"))

        chispa.assert_df_equality(actual, expected)

    def test_ignore_weekend_on_weekday(self):
        # Arrange
        data = [(1, 24.0, False, datetime.datetime(2021, 9, 10, 00, 00, 00),
                 datetime.datetime(2021, 9, 13, 00, 00, 00))]
        schema = self.schema
        df = self.spark.createDataFrame(data=data, schema=schema)

        # Act
        actual = self.daily_split(df, [
            datetime.datetime(2021, 9, 10, 00, 00, 00),
            datetime.datetime(2021, 9, 13, 00, 00, 00)
        ])

        # Assert
        expected = self.spark.createDataFrame(
            [],
            schema)\
            .withColumn("started_date", F.col("started").cast("date")) \
            .withColumn("finished_date", F.col("finished").cast("date"))

        chispa.assert_df_equality(actual, expected)


if __name__ == '__main__':
    unittest.main()
