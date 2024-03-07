from pyspark.sql import SparkSession
from commons.configurations.configurator import Configurator
from commons.db.query_runner import QueryRunner
from commons.db.sqlalchemyutils import create_engine
from commons.spark.connection_details import get_connection_details
from commons.spark.spark_db_data_source import SparkDBDataSource
from commons.spark.spark_db_sink import SparkDBSink
from datasources.data_source import DataSource
from main import DailyProcessorRunner

from processor.daily_processor import config as cfg


def get_runner(config):
    spark = SparkSession.builder.appName("daily-processor").getOrCreate()

    connection_details = get_connection_details(config)

    spark_db_sink = SparkDBSink(config, connection_details, spark)

    spark_data_source = SparkDBDataSource(
        config, connection_details, spark
    )

    engine = create_engine(config)

    query_runner = QueryRunner(config, engine)

    data_source = DataSource(config, spark_data_source)

    return DailyProcessorRunner(config, engine, data_source,
                                spark_db_sink, query_runner)


def main():
    config = Configurator().get_configurator()
    runner = get_runner(config)
    runner.run(
        source_table=config.job.processing.daily_processor.daily_proc_source_table or cfg.TIME_REPORTS_TABLE,
        expected_calendar_time_table=cfg.WORKING_HOURS
    )


if __name__ == "__main__":
    main()
