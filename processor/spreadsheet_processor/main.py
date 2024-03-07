from commons.logging.simple_logger import logger
from spark_spread_sheets_datasource import SparkSpreadsheetsDataSource


class SpreadsheetRunner:
    def __init__(self, spreadsheet, spark_session, spark_data_source,
                 target_table, mapper, spark_db_sink) -> None:
        self.spreadsheet = spreadsheet
        self.spark_session = spark_session
        self.spark_data_source = spark_data_source
        self.target_table = target_table
        self.mapper = mapper
        self.spark_db_sink = spark_db_sink

    def run(self) -> None:
        data = self.spreadsheet.get_data()

        spreadsheets_df = SparkSpreadsheetsDataSource(
            self.spark_session,
            data).get_dataframe()
        logger.info("Created dataframe from received data from spreadsheets")

        target_table = self.spark_data_source.load(
            table_name=self.target_table,
            predicates="")
        logger.info(f"Created dataframe from target table '{self.target_table}'")

        try:
            self.spark_db_sink.save(
                self.mapper.cast_schema(
                    current_table=spreadsheets_df,
                    target_table=target_table),
                self.target_table
            )
            logger.info(f"Saved data to '{self.target_table}'")
        except RuntimeError as e:
            logger.error(f"{e}")
