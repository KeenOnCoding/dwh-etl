from commons.logging.simple_logger import logger


class DataSource:

    def __init__(self, config, spark_data_source):
        self.target_table = config.job.processing.daily_processor.daily_proc_target_table
        logger.info(f"Initializing Datasource with target "
                    f"table {self.target_table}")

        self.spark_data_source = spark_data_source

    def extract_data(self, source_table, predicates=None):
        logger.info(f"Extracting data from target table [{source_table}]"
                    f" with following predicates: {predicates}")

        return self.spark_data_source.load(source_table, predicates)
