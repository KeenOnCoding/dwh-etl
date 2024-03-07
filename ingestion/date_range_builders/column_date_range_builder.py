from datetime import datetime, date

from commons.logging.simple_logger import logger
from ingestion.constants import DEFAULT_DATETIME_PATTERN
from ingestion.date_range_builders.date_operations import add_offset
from ingestion.date_range_builders.date_range_builder import IncrementalPayloadBuilder
from ingestion import constants


class ColumnIncrementalPayloadBuilder(IncrementalPayloadBuilder):
    DEFAULT_BATCH_WINDOW = 48
    DEFAULT_DATE_COLUMN = 'modification_date'
    DEFAULT_BATCH_WINDOW_UNIT = constants.HOURS

    def __init__(self, config, query_runner):
        IncrementalPayloadBuilder.__init__(self, config, query_runner)

    def batch_window(self):
        return IncrementalPayloadBuilder.batch_window(self) or self.DEFAULT_BATCH_WINDOW

    def date_column(self):
        return IncrementalPayloadBuilder.date_column(self) or self.DEFAULT_DATE_COLUMN

    def date_from_time_offset(self):
        return IncrementalPayloadBuilder.date_from_time_offset(self)

    def batch_window_unit(self):
        return IncrementalPayloadBuilder.batch_window_unit(self) or self.DEFAULT_BATCH_WINDOW_UNIT

    def build(self):
        date_from_time_offset = self.date_from_time_offset()

        modification_date_from = self.query_runner.query_single_result(
            f"select max({self.date_column()}) from {self.target_table()}")

        if modification_date_from is None:
            default_value = self.default_partition_value()
            if default_value is None:
                raise ValueError(
                    "DEFAULT_PARTITION_VALUE is not set."
                )
            logger.info("Target table is empty. "
                        f"Using default partition value as {default_value}")

            modification_date_from = datetime.strptime(
                default_value,
                DEFAULT_DATETIME_PATTERN,
            )

        modification_date_from = add_offset(modification_date_from,
                                            date_from_time_offset,
                                            self.batch_window_unit())

        modification_date_to = add_offset(modification_date_from,
                                          self.batch_window(),
                                          self.batch_window_unit())

        logger.info(f'Generated date range: '
                    f'date_from = {modification_date_from}, '
                    f'date_to = {modification_date_to}')

        return modification_date_from, modification_date_to
