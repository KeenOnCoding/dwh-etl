from abc import ABC, abstractmethod
from typing import List

from commons.utils.config_query_runner import ConfigQueryRunner


class IncrementalRuntimeConfig(ConfigQueryRunner):

    def __init__(self, config, query_runner=None):
        ConfigQueryRunner.__init__(self, config, query_runner)

    def batch_window(self):
        return self.config.job.ingestion.fetching_strategy.batch_window

    def date_column(self):
        return self.config.job.ingestion.fetching_strategy.date_column

    def date_from_time_offset(self):
        return self.config.job.ingestion.fetching_strategy.date_from_time_offset

    def granularity(self):
        return self.config.job.ingestion.fetching_strategy.granularity

    def batch_window_unit(self):
        return self.config.job.ingestion.fetching_strategy.batch_window_unit or self.granularity()

    def default_partition_value(self):
        return self.config.job.ingestion.fetching_strategy.default_partition_value

    def target_table(self):
        return self.config.job.ingestion.general.user_table or self.config.job.ingestion.general.target_table


class IncrementalPayloadBuilder(IncrementalRuntimeConfig, ABC):
    FROM_PARAM = 'dateFrom'
    TO_PARAM = 'dateTo'

    @abstractmethod
    def build(self) -> List[dict]:
        raise NotImplemented


class IncrementalBuilderApiAdapter:
    """
    The Adapter is a temporary solution for the other date range builder classes
    to conform the updated DateRangeBuilder api method (before DateRangeBuilder#build was returning
    dictionary object, now - list of dictionaries)

    See date_range_builder_factory for actual usage.
    """

    def __init__(self, date_range_builder: IncrementalPayloadBuilder):
        self.date_range_builder = date_range_builder

    def build(self) -> List[dict]:
        out = self.date_range_builder.build()
        if isinstance(out, list):
            return out

        return [out]
