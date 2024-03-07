from pyspark.sql import SparkSession
from commons.configurations.configurator import Configurator
from commons.spark.connection_details import get_connection_details
from commons.spark.spark_db_sink import SparkDBSink
from main import NlmsReportRunner


def get_runner():

    config = Configurator().get_configurator()
    conn_props = get_connection_details(config=config)
    spark_session = SparkSession.builder.appName("nlms-processor").getOrCreate()

    target_table = config.job.processing.nlms_report_processor.nlms_report_target_table
    trigger_url = config.job.processing.nlms_report_processor.trigger_url
    check_status_url = config.job.processing.nlms_report_processor.check_status_url
    on_prem_report_path = config.job.processing.nlms_report_processor.on_prem_report_path
    http_report_url = config.job.processing.nlms_report_processor.http_report_url
    header_key = config.job.processing.nlms_report_processor.header_key

    spark_db_sink = SparkDBSink(config=config,
                                connection_properties=conn_props,
                                spark=spark_session)

    return NlmsReportRunner(trigger_url=trigger_url,
                            check_status_url=check_status_url,
                            on_prem_report_path=on_prem_report_path,
                            http_report_url=http_report_url,
                            spark_session=spark_session,
                            target_table=target_table,
                            spark_db_sink=spark_db_sink,
                            header_key=header_key)


def main():
    get_runner().run()


if __name__ == "__main__":
    main()
