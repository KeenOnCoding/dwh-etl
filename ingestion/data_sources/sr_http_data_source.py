import csv
from io import StringIO
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StringType, StructType

from ingestion.data_sources.http_data_source import HttpDataSource


class SrHttpDataSource(HttpDataSource):
    def __init__(self, query_params_builder, config, spark, auth):
        HttpDataSource.__init__(self, query_params_builder)
        self.config = config
        self.spark = spark
        self.auth = auth

    def __transform_header(self, column):
        return column.lower().replace(' ', '_').replace('-', '_')

    def __parse_csv_header_to_df_field(self, column):
        return StructField(column, StringType(), True)

    def __blank_as_null(self, col):
        return F.when(F.col(col) != "", F.col(col)).otherwise(None)

    def _read_data(self, response):
        csv_io = StringIO(response.text)
        csv_reader = csv.reader(csv_io, delimiter=',', quotechar='"')
        csv_header = [self.__transform_header(column) for column in
                      next(csv_reader)]
        csv_file = pd.read_csv(csv_io, keep_default_na=False)

        df_fields = [self.__parse_csv_header_to_df_field(column) for column in
                     csv_header]

        df_schema = StructType(df_fields)

        response_df = self.spark.createDataFrame(csv_file, df_schema)
        query = [self.__blank_as_null(x).alias(x) for x in response_df.columns]
        return response_df.select(*query)

    def _enrich_request(self, request, **kwargs):
        request['headers'] = {'Accept': 'text/csv;charset=utf-8'}
        return request
