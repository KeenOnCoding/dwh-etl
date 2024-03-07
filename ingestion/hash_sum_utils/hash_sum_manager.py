from collections import namedtuple
from datetime import datetime

from commons.db.query_runner import QueryRunner
from commons.logging.simple_logger import logger
from commons.utils.stringconverters import \
    mnemonic_to_absolute_sec
from commons.utils.stringconverters import mnemonic_str_to_timedelta

HashsumRecord = namedtuple('HashSumCheckerPayload',
                           ['batch_id', 'table_name',
                            'request_parameters', 'hash_sum', 'date_from',
                            'date_to'])


class HashSumManager:
    """
        Class for working with a check sum got from an API response
    """

    def __init__(self, *, hash_sum_table, target_table, query_runner,
                 window="0d",
                 request_parameter_parser=None):
        self.util_table = hash_sum_table
        self.target_table = target_table
        self.query_runner: QueryRunner = query_runner
        self.window_delta_in_seconds = mnemonic_to_absolute_sec(
            window)
        self.window_timedelta = mnemonic_str_to_timedelta(window)
        self.request_parameter_parser = request_parameter_parser

    # deprecated, remove in future, use insert instead
    def save(self, hash_sum, batch_id, hash_key):
        if self.util_table is None:
            return

        self.query_runner.execute(
            f"MERGE {self.util_table} AS [target] "
            f"USING ("
            f"SELECT {batch_id} batch_id, \'{self.target_table}\' table_name, "
            f"\'{hash_key}\' request_parameters, \'{hash_sum}\' hash_sum"
            f") AS [source] "
            "ON [target].table_name = [source].table_name "
            "AND [target].request_parameters = [source].request_parameters "
            "AND [target].hash_sum = [source].hash_sum "
            "WHEN MATCHED THEN "
            "UPDATE SET [target].batch_id = [source].batch_id "
            "WHEN NOT MATCHED THEN "
            "INSERT (batch_id, table_name, request_parameters, hash_sum)"
            " VALUES ([source].batch_id, [source].table_name, "
            "[source].request_parameters, [source].hash_sum);"
        )

    def save_one(self, hash_sum, batch_id, request_params):
        if self.util_table is None:
            return
        logger.info(f'Saving key [{request_params}] with hashsum {hash_sum}')
        self.query_runner.execute(
            f"INSERT INTO {self.util_table} "
            f"(batch_id, table_name, request_parameters, hash_sum) "
            f"VALUES ('{batch_id}', '{self.target_table}', '{request_params}',"
            f" '{hash_sum}');"
        )

    # NOTE: after the ingestion, if there were data changes, the hashsum
    # table contains updated hashsums with the newest (previous batch id)
    # so the data is changing frequently, we will be re-pulling old data
    # continuously. It's okay for now, but the desired contract is different.
    # For example, if we have data mismatch with batch id 10, we are pulling
    # new data, erasing old record in hashsum table and now this record has
    # new hashsum and updated batch, for example, 25.
    # So the proper solution should not change old batch ids, making the
    # records static in time, so the data that belongs to some data range
    # should stay in this range.
    #
    # UPD: the data is still being fetched from the utility table with the
    # filter current_batch_id - window_delta_in_seconds, but then, when we are
    # mapping table records to type we additionally filter them by the
    # date_from parameter with the following filter:
    # date_from (from utility table) should be >= current_date - window
    # meaning we are only getting those records, which are in the proper
    # date range window.
    #
    # The implementation might be adjusted to immediately fetch
    # proper records but this requires a bit major changes, including
    # changes in utility table schema, changes in other places where we are
    # interacting with this utility table
    def load(self, batch_id):
        if self.util_table is None:
            return []

        query_result = self.query_runner.query(
            f'SELECT * FROM {self.util_table} '
            f'WHERE table_name = \'{self.target_table}\' '
            f'AND batch_id > '
            f'{batch_id - self.window_delta_in_seconds}'
        )
        if not query_result:
            return []

        current_date_from_batch = \
            datetime.fromtimestamp(batch_id)
        logger.info(f"Using current_date_from_batch: {current_date_from_batch}")

        base_date_from = current_date_from_batch - self.window_timedelta
        logger.info(f"Using base_date_from: {base_date_from}")

        mapped_output = list()
        for row in query_result:
            table_name = row[0]
            hash_sum = row[1]
            request_parameters = row[2]
            batch_id = row[3]

            date_from, date_to = self.request_parameter_parser.parse_dates(
                request_parameters)
            logger.info(f"Parsed request parameters, got data_from: {date_from}; date_to: {date_to}")

            if date_to >= base_date_from:
                mapped_output.append(
                    HashsumRecord(batch_id, table_name, request_parameters,
                                  hash_sum, date_from, date_to)
                )
        return mapped_output
