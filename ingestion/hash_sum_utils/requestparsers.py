from datetime import datetime

from commons.logging.simple_logger import logger


class GenericRequestParser:

    @staticmethod
    def parse_request_string_to_dict(request_parameters_string):
        return dict((kv.split('=') for kv in request_parameters_string.split('&') if kv != ""))


class FromToRequestParser(GenericRequestParser):

    def __init__(self, from_to_param_names, date_format):
        logger.info(f"Using date_format: {date_format},"
                    f" from_to_param_names: '{from_to_param_names}' ")

        if from_to_param_names is None:
            logger.warning(
                'No from-to-param names found. Any call to the instance of this class will results in exception.')
            split = [None, None]
        else:
            split = from_to_param_names
            if len(split) == 1:
                logger.info(f'Only one parameter found: {split}')
                split = [split[0], split[0]]

        self.from_param_name, self.to_param_name = split
        self.date_format = date_format

    def parse_dates(self, request_parameters_string):
        mapping = FromToRequestParser.parse_request_string_to_dict(
            request_parameters_string)

        return datetime.strptime(mapping[self.from_param_name],
                                 self.date_format), \
               datetime.strptime(mapping[self.to_param_name], self.date_format)
