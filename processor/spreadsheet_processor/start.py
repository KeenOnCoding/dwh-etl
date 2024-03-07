from pyspark.sql import SparkSession

from commons.configurations.configurator import Configurator
from commons.spark.connection_details import get_connection_details
from commons.spark.spark_db_data_source import SparkDBDataSource
from commons.spark.spark_db_sink import SparkDBSink
from main import SpreadsheetRunner
from schema_mapping import SchemaMapper
from spreadsheet import Spreadsheet


def get_runner():
    config = Configurator().get_configurator()

    spark_session = SparkSession.builder.appName("spreadsheet-processor") \
        .getOrCreate()

    spreadsheet = Spreadsheet(
        credentials=config.job.processing.spreadsheets_processor.google_creds,
        scopes=config.job.processing.spreadsheets_processor.scope,
        spreadsheet_id=config.job.processing.spreadsheets_processor.spreadsheet_id,
        range=config.job.processing.spreadsheets_processor.range
    )
    target_table = config.job.processing.spreadsheets_processor.target_table

    connection_details = get_connection_details(config)

    spark_db_datasource = SparkDBDataSource(
        config, connection_details, spark_session
    )

    mapper = SchemaMapper()

    spark_db_sink = SparkDBSink(
        config, connection_details, spark_session
    )

    return SpreadsheetRunner(
        spreadsheet=spreadsheet,
        spark_session=spark_session,
        spark_data_source=spark_db_datasource,
        target_table=target_table,
        mapper=mapper,
        spark_db_sink=spark_db_sink
    )


def main():
    get_runner().run()


if __name__ == '__main__':
    main()
