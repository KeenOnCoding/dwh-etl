from datetime import datetime
from commons.utils.stringconverters import dhrms_string_to_timedelta
from lag.lag_payload import LagPayload


class NoDataException(Exception):
    pass


class NoSuchStrategyException(Exception):
    pass


class LagCheckResponse:
    __slots__ = ('check_payload', 'lag_value', 'status')

    def __init__(self, check_payload: LagPayload,
                 lag_value, status):
        self.check_payload = check_payload
        self.lag_value = lag_value
        self.status = status

    def get_description(self) -> str:
        return "Table: {}.{}\n" \
               "Threshold: {}\n" \
               "Lag value: {}\n" \
               "Status: {}\n" \
            .format(self.check_payload.schema,
                    self.check_payload.table_name,
                    self.check_payload.threshold_value,
                    self.lag_value,
                    self.status)

    def lag(self):
        return int(self.lag_value.total_seconds())


class LagChecker:
    __slots__ = ('query_runner', 'current_date')

    def __init__(self, query_runner):
        self.query_runner = query_runner
        self.current_date = datetime.now()

    def check(self, check_payload: LagPayload) -> LagCheckResponse:
        full_table_name = f"{check_payload.schema}.{check_payload.table_name}"

        partition_column_max_value = self.__read_variable(
            full_table_name,
            check_payload.partition_column
        )

        if partition_column_max_value is None:
            raise NoDataException(f"No data in table: {full_table_name}")

        lag, status = self.__check_according_to_strategy(
            partition_column_max_value,
            check_payload.check_strategy,
            check_payload.threshold_value
        )

        return LagCheckResponse(
            check_payload=check_payload,
            lag_value=lag,
            status=status
        )

    def __read_variable(self, table_name, partition_column):
        return self.query_runner \
            .query_single_result(f"SELECT MAX({partition_column}) "
                                 f"FROM {table_name}")

    def __check_according_to_strategy(self, partition_value,
                                      strategy,
                                      threshold):

        if strategy == 'batch':
            return self.__check_by_batch_strategy(partition_value, threshold)
        elif strategy == 'date':
            return self.__check_by_date_strategy(partition_value, threshold)
        else:
            raise NoSuchStrategyException(f'Strategy {strategy} is not found')

    def __check_by_date_strategy(self, partition_value, threshold):
        lag = self.current_date - partition_value
        is_lag = lag < dhrms_string_to_timedelta(threshold)
        if is_lag:
            return lag, "OK"
        else:
            return lag, "FAILURE"

    def __check_by_batch_strategy(self, partition_value, threshold):
        datetime_from_batch = datetime.fromtimestamp(partition_value)
        return self.__check_by_date_strategy(datetime_from_batch, threshold)

