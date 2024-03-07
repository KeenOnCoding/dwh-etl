from commons.logging.simple_logger import logger
import time


class DimensionCleanupRunner:

    def __init__(self, config, query_runner):
        self.query_runner = query_runner
        self.config = config
        self.secs_in_day = 86400

    def run(self):
        retention_window = self.config.utility.dimension_cleanup.retention_time
        table = self.config.utility.dimension_cleanup.dim_cleanup_target_table
        partition_column = self.config.utility.dimension_cleanup.dim_cleanup_partition_column
        logger.info(f"Partition_column {partition_column}, Target_table {table}, Retention_window {retention_window}")
        threshold = int(time.time()) - (self.secs_in_day
                                        * int(retention_window))

        self.query_runner.execute(f"DELETE FROM {table}  "
                                  f"WHERE {partition_column} < {threshold};")
