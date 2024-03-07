from datetime import datetime, date
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from ingestion.query_params_builders.query_params_builder import QueryParamsBuilder
from commons.logging.simple_logger import logger


# TODO do not automatically calculate start_year and start_week
# instead - fetch parameters from the batch_context
class WeekYearQueryParamsBuilder(QueryParamsBuilder):
    def __init__(self, config):

        start_year = config.job.ingestion.fetching_strategy.start_year
        start_week = config.job.ingestion.fetching_strategy.start_week

        if start_year is None or start_week is None:
            self.current_time = datetime.today()
        else:
            self.current_time = date(int(start_year), 1, 1) + relativedelta(
                weeks=int(start_week))
        self.delta = timedelta(weeks=1)
        logger.info(f"Using start year [{start_year}] and start week "
                    f"[{start_week}]")

    def build(self, batch_context) -> str:
        current_year, current_week, _ = self.current_time.isocalendar()

        query_string = f"year={current_year}" \
                       f"&week_number={current_week}"

        self.current_time += self.delta

        return query_string
