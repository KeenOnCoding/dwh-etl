from ingestion.query_params_builders.from_to_query_params_builder import FromToQueryParamsBuilder
from ingestion.query_params_builders.empty_query_param_builder import EmptyQueryParamsBuilder
from ingestion.query_params_builders.week_year_query_params_builder import WeekYearQueryParamsBuilder
from commons.logging.simple_logger import logger
from commons.utils.validators import require_not_none


class QueryParamsBuilderFactory:
    def __init__(self, config):
        self.config = config

    def create(self):
        fetching_strategy = require_not_none(self.config.job.ingestion.fetching_strategy.fetching_strategy,
                                             'fetching strategy must be set')

        logger.info(f'Using {fetching_strategy} strategy')

        if fetching_strategy == 'SNAPSHOT':
            return EmptyQueryParamsBuilder()
        elif fetching_strategy == 'INCREMENTAL':
            return FromToQueryParamsBuilder(self.config)
        elif fetching_strategy == 'WEEK-YEAR':
            return WeekYearQueryParamsBuilder(self.config)

        raise RuntimeError(f'Fetching strategy is incorrect: {fetching_strategy}')
