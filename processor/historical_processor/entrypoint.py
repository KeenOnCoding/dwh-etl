from operator import methodcaller

from pyspark.sql import SparkSession
from commons.configurations.configurator import Configurator
from commons.spark.connection_details import get_connection_details
from commons.spark.spark_db_data_source import SparkDBDataSource
from commons.spark.spark_db_sink import SparkDBSink
from commons.logging.simple_logger import logger
from historicizer import Historicizer


def get_runner(config):
    spark = SparkSession.builder \
        .appName('historical-copy') \
        .getOrCreate()

    from multiprocessing import cpu_count
    # set number of shuffle partitions to amount of CPU's
    # cos we're running in local mode
    spark.conf.set('spark.sql.shuffle.partitions', str(cpu_count()))

    history_table_name = config.job.processing.history_processor.history_table
    staging_table_name = config.job.processing.history_processor.staging_table

    raw_staging_req_field_mappings = config.job.processing.history_processor.staging_req_field_mappings
    raw_history_req_field_mappings = config.job.processing.history_processor.history_req_field_mappings

    connection_details = get_connection_details(config)

    df_reader = SparkDBDataSource(config, connection_details, spark)
    df_appender = SparkDBSink(config, connection_details, spark)

    staging_req_field_mappings = mapping_dict(raw_staging_req_field_mappings)
    history_req_field_mappings = mapping_dict(raw_history_req_field_mappings)

    logger.info(f"HISTORY_TABLE: {history_table_name}")

    historizer = Historicizer(df_reader=df_reader,
                              df_appender=df_appender,
                              history_table_name=history_table_name,
                              staging_table_name=staging_table_name,
                              history_field_mapping=history_req_field_mappings,
                              staging_field_mapping=staging_req_field_mappings)

    in_history, not_in_history = historizer.historicize()
    logger.info(f"New records with unstaged IDs is {not_in_history}")
    logger.info(f"New records with staged IDs is {in_history}")


def main():
    config = Configurator().get_configurator()
    get_runner(config)


def mapping_dict(mapping: str):
    if mapping is not None:
        return {i["from"]: i["to"] for i in mapping}


if __name__ == "__main__":
    main()
