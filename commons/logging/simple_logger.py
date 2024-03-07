import logging
import os.path
import traceback
from logging import INFO


class Logger:
    """
    Logger class adding verbose logging to subclasses.
    Subclasses get info(), debug(), warning() and error() methods which,
    alongside the information given, also show location of the message
    (file, line and function).
    """

    def __init__(self):
        logging.basicConfig(level=INFO,
                            format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    # Formats the message and calls logging method to handle it
    def _log_message(self, logfn, message):
        file_line = ''
        method = ''
        trace_back_stack = traceback.extract_stack()

        if len(trace_back_stack) > 2:
            filename = os.path.basename(trace_back_stack[-3][0])
            line = trace_back_stack[-3][1]
            file_line = f'({filename}:%{line}):'

            method = trace_back_stack[-3][2]
            if method != '<module>':
                method += '()'

        logfn(file_line + method + ': ' + message)

    def info(self, message):
        self._log_message(logging.info, message)

    def debug(self, message):
        self._log_message(logging.debug, message)

    def warning(self, message):
        self._log_message(logging.warning, message)

    def error(self, message):
        self._log_message(logging.error, message)


logger = Logger()
