from pyspark.sql import SparkSession, DataFrame

from commons.logging.simple_logger import logger
from commons.monitoring.prometheus_manager import PrometheusManager
from commons.spark.connection_details import get_connection_details
from commons.spark.spark_db_data_source import SparkDBDataSource
from configurations.configurator import Configurator


def main():
    config = Configurator().get_configurator()
    job_name = 'count-check-job'
    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    connection_details = get_connection_details(config)
    data_source = SparkDBDataSource(config, connection_details, spark)

    table = config.utility.count_check.count_check_table
    metric = config.utility.count_check.count_metric_name

    logger.info(f"Table: {table}, Metric: {metric}")

    df: DataFrame = data_source.load(table, predicates=None)

    env_label = config.environment.shared.env
    labels = [('env', env_label), ('table', table)]

    prometheus_manager = PrometheusManager(
        port=9091,
        labels=labels,
        suppress=env_label != 'prod')

    amount_of_errs = df.count()
    logger.info(f'Amount of records in {table}: {amount_of_errs}')

    prometheus_manager.send_counter(job_name,
                                    metric,
                                    'Threshold Value',
                                    amount_of_errs)


if __name__ == '__main__':
    main()
