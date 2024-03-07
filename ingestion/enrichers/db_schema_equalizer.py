from pyspark.sql import DataFrame

from commons.logging.simple_logger import logger
from commons.spark.spark_utils import df_string
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class DbSchemaEqualizer(AbstractEnricher):
    """
    DbSchemaEqualizer is responsible for table column alignment when turned on.
    This class assumes that the target table is present in the database.

    When turned on this class fetches all the the columns that present in target
    table from the database and drops those columns in the dataframe that don't
    present in the database table
    """

    def __init__(self, config, query_runner):
        self.is_track_db_schema = config.job.ingestion.enrichers.track_db_schema
        self.target_table = config.job.ingestion.general.target_table
        self.query_runner = query_runner

    def __split_table_name(self, full_table_name):
        split_name = full_table_name.split('.')
        table_name = split_name[-1]
        table_schema = split_name[-2] if len(split_name) > 1 else 'dbo'

        return table_schema, table_name

    def __get_db_table_schema(self, full_table_name: str):
        logger.info(f"Using table '{full_table_name}' schema for received data schema equalizing")
        split_table_name = self.__split_table_name(full_table_name)
        table_schema = split_table_name[0]
        table_name = split_table_name[1]

        query = f"SELECT COLUMN_NAME " \
                f"FROM INFORMATION_SCHEMA.COLUMNS " \
                f"WHERE TABLE_NAME = \'{table_name}\' " \
                f"AND TABLE_SCHEMA = \'{table_schema}\'"
        response = self.query_runner.query(query)
        table_fields = [item[0] for item in response]

        return table_fields

    def __get_schema_diff(self, df_schema, table_schema):
        df_attributes = set(df_schema)
        schema_attributes = set(table_schema)
        attributes_to_drop = list(df_attributes - schema_attributes)
        logger.info(f"Found attributes to drop: {attributes_to_drop}")
        return attributes_to_drop

    def enrich(self, df: DataFrame, context):
        logger.info("Using DbSchemaEqualizer: equalizing schema before calculating hash sum of each record")
        if self.is_track_db_schema:
            table_schema = self.__get_db_table_schema(self.target_table)
            df_schema = df.schema.names
            mismatched_fields = self.__get_schema_diff(df_schema, table_schema)
            if mismatched_fields:
                logger.warning(f'The fields are not found in table schema: {mismatched_fields}. Fields sample:\n'
                               f'{df_string(df.select(mismatched_fields), 5, truncate=False)}')

                df = df.drop(*mismatched_fields)
                logger.info("Mismatched fields ware dropped")

        return df
