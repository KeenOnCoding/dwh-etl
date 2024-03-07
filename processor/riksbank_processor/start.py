from pyspark.sql import SparkSession
from commons.db.sqlalchemyutils import create_engine
from commons.configurations.configurator import Configurator
from commons.spark.connection_details import get_connection_details
from commons.spark.spark_db_sink import SparkDBSink
from main import RiksBankCrossRatesRunner


def get_runner():

    config = Configurator().get_configurator()
    conn_props = get_connection_details(config=config)
    spark_session = SparkSession.builder.appName("riksbank-processor").getOrCreate()
    engine = create_engine(config)

    # from json
    obs_url = config.job.processing.riksbank_crossrate_processor.observation_url
    cross_url = config.job.processing.riksbank_crossrate_processor.crossrate_url
    default_date = config.job.processing.riksbank_crossrate_processor.default_partition_value
    target_table = config.job.processing.riksbank_crossrate_processor.riksbank_currencies_target_table
    latest_table = config.job.processing.riksbank_crossrate_processor.riksbank_currencies_latest_table
    currency_codes_source_table = config.job.processing.riksbank_crossrate_processor.currency_codes_source_table
    sleep_duration = config.job.processing.riksbank_crossrate_processor.sleep_duration
    build_number = config.environment.shared.build_number
    build_url = config.environment.shared.build_url

    spark_db_sink = SparkDBSink(config=config,
                                connection_properties=conn_props,
                                spark=spark_session)

    return RiksBankCrossRatesRunner(obs_url=obs_url,
                                    cross_url=cross_url,
                                    default_date=default_date,
                                    latest_table=latest_table,
                                    spark_session=spark_session,
                                    target_table=target_table,
                                    spark_db_sink=spark_db_sink,
                                    sleep_duration=sleep_duration,
                                    query_config=config,
                                    query_engine=engine,
                                    currency_codes_source_table=currency_codes_source_table,
                                    build_number=build_number,
                                    build_url=build_url)


def main():
    get_runner().run()


if __name__ == "__main__":
    main()
