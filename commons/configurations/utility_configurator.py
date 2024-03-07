from configurations.utility import is_exist


class UtilityConfigurator:
    def __init__(self, data_lag=None, dimension_cleanup=None, count_check=None):
        self.data_lag = DataLagConfigurator(**is_exist(data_lag))
        self.dimension_cleanup = DimensionCleanup(**is_exist(dimension_cleanup))
        self.count_check = CountCheck(**is_exist(count_check))


class DataLagConfigurator:
    def __init__(self, data_lag_suppress_prometheus=None, metric_name=None, rules=None):
        self.data_lag_suppress_prometheus = data_lag_suppress_prometheus
        self.metric_name = metric_name
        self.rules = rules


class DimensionCleanup:
    def __init__(self, dim_cleanup_target_table=None, dim_cleanup_partition_column=None, retention_time=None):
        self.dim_cleanup_target_table = dim_cleanup_target_table
        self.dim_cleanup_partition_column = dim_cleanup_partition_column
        self.retention_time = retention_time


class CountCheck:
    def __init__(self, count_check_table=None, count_metric_name=None):
        self.count_check_table = count_check_table
        self.count_metric_name = count_metric_name
