import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from ingestion.enrichers.abstract_enricher import AbstractEnricher
from commons.logging.simple_logger import logger


class ListParserEnricher(AbstractEnricher):
    """
        Bonobo transofmation class

        Is aimed to transform nested list columns to single string

        Column names are passed through parameter 'COLUMNS_TO_PARSE'

        If parameter is empty or point to single value columns
        no transformation is performed

        Warning! Currently enricher supports only strings without any
        special symbols or commas. Using them in strings leads to
        an unpredictable behaviour

        Example:
            params: 'COLUMNS_TO_PARSE' is set to 'col1'
            input: {"col1": ["val1", "val2"], "col2": "val"}
            output: {"col1": "val1,val2", "col2": "val"}
    """

    def __init__(self, config):
        self.columns_to_parse_list = config.job.ingestion.enrichers.columns_to_parse

    def __parse_column(self, col):
        if isinstance(col, list):
            return ','.join(col)
        else:
            return col

    def enrich(self, data: DataFrame, context):
        logger.info(f"Using ListParserEnricher: transforming nested list columns '{self.columns_to_parse_list}'"
                    f" to single string")
        if self.columns_to_parse_list:

            parse = F.udf(self.__parse_column)

            for column_to_parse in self.columns_to_parse_list:
                try:
                    data = data.withColumn(
                        column_to_parse,
                        parse(F.col(column_to_parse))
                    )
                except AnalysisException:
                    continue

        return data
