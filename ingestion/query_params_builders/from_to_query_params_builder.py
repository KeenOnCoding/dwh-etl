from commons.utils.validators import require_not_none
from commons.logging.simple_logger import logger

from ingestion.query_params_builders.query_params_builder import QueryParamsBuilder


class FromToQueryParamsBuilder(QueryParamsBuilder):
    def __init__(self, config):
        self.date_format = config.job.ingestion.general.date_format
        self.params_names = require_not_none(
            config.job.ingestion.general.from_to_params_names,
            'from_to_params_names is required')

    def build(self, batch_context):
        date_from, date_to = batch_context.get_from_to_dates()
        logger.info(f"Using date_from: '{date_from}'; Using date_to: '{date_to}'")

        parameters = dict(zip(self.params_names, [date_from, date_to]))

        if self.date_format:
            return '&'.join(key + '=' + val.strftime(self.date_format) for key, val in parameters.items())

        return '&'.join(key + '=' + val.isoformat() for key, val in parameters.items())
