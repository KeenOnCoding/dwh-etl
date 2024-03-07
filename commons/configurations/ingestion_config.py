from configurations.utility import is_exist


class IngestionConfiguration:

    def __init__(self, general: object = None, auth: object = None, fetching_strategy: object = None,
                 enrichers: object = None, data_manager: object = None, data_source: object = None):
        self.general = GeneralConfiguration(**is_exist(general))
        self.auth = AuthConfiguration(**is_exist(auth))
        self.fetching_strategy = FetchingStrategyConfiguration().unpack(**is_exist(fetching_strategy))
        self.enrichers = EnricherConfiguration(**is_exist(enrichers))
        self.data_manager = DataManagerConfig(**is_exist(data_manager))
        self.data_source = DataSourceConfig(**is_exist(data_source))


class AuthConfiguration:

    def __init__(self, auth_strategy=None, auth_url=None, client_id=None,
                 client_secret=None, api_user=None, api_password=None,
                 scope=None):
        self.auth_strategy = auth_strategy
        self.auth_url = auth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_user = api_user
        self.api_password = api_password
        self.scope = scope


class GeneralConfiguration:

    def __init__(self, job_name=None, target_table=None, user_table=None,
                 date_format=None, from_to_params_names=None, start_week=None,
                 fetch_pause=None, calendar_table=None, partition_columns=None):
        self.job_name = job_name
        self.target_table = target_table
        self.user_table = user_table
        self.date_format = date_format
        self.from_to_params_names = from_to_params_names
        self.start_week = start_week
        self.fetch_pause = fetch_pause
        self.calendar_table = calendar_table
        self.partition_columns = partition_columns


class FetchingStrategyConfiguration:

    def __init__(self, fetching_strategy=None, incremental_strategy=None, batch_window=None,
                 batch_window_unit=None, date_from_time_offset=None, granularity=None,
                 date_range_upper_bound=None, align_dates=False, date_column=None,
                 default_partition_value=None, backfill_upper_offset=None, backfill_safe_window=None,
                 backfill_max_ranges_per_run=-1, backfill_left_bound_kick_forward=None,
                 backfill_crop_now=False, start_year=None, start_week=None):
        self.fetching_strategy = fetching_strategy
        self.incremental_strategy = incremental_strategy
        self.batch_window = batch_window
        self.batch_window_unit = batch_window_unit
        self.date_from_time_offset = date_from_time_offset
        self.granularity = granularity
        self.date_range_upper_bound = date_range_upper_bound
        self.align_dates = align_dates
        self.date_column = date_column
        self.backfill_upper_offset = backfill_upper_offset
        self.backfill_safe_window = backfill_safe_window
        self.backfill_max_ranges_per_run = backfill_max_ranges_per_run
        self.backfill_left_bound_kick_forward = backfill_left_bound_kick_forward
        self.backfill_crop_now = backfill_crop_now
        self.default_partition_value = default_partition_value
        self.start_year = start_year
        self.start_week = start_week

    def unpack(self, fetching_strategy="SNAPSHOT", week_year=None, incremental=None):
        params = dict()
        params.update({"fetching_strategy": fetching_strategy})
        if week_year:
            params.update(week_year)
            return FetchingStrategyConfiguration(**params)
        if incremental:
            self.incremental_strategy = incremental["incremental_strategy"]
            params.update({"incremental_strategy": self.incremental_strategy})
            params.update(incremental["general"])
            if self.incremental_strategy == "BACKFILL" and "backfill" in incremental:
                params.update(incremental["backfill"])
            return FetchingStrategyConfiguration(**params)
        return FetchingStrategyConfiguration(fetching_strategy=fetching_strategy)


class EnricherConfiguration:

    def __init__(self, track_db_schema=False, hash_column=None, have_hash_sum=False,
                 date_column_to_datetime=None, date_column_origin_format=None,
                 stringify_json_columns=None, columns_to_parse=None,
                 surrogate_key_columns=None, field_mapping=None, explicit_cast=None):
        self.track_db_schema = track_db_schema
        self.hash_column = hash_column
        self.have_hash_sum = have_hash_sum
        self.date_column_to_datetime = date_column_to_datetime
        self.date_column_origin_format = date_column_origin_format
        self.stringify_json_columns = stringify_json_columns
        self.columns_to_parse = columns_to_parse
        self.surrogate_key_columns = surrogate_key_columns
        self.field_mapping = field_mapping
        self.explicit_cast = explicit_cast


class DataManagerConfig:

    def __init__(self, date_manager_mode="INACTIVE", data_manager_window=None,
                 hash_sum_checker_impl=None, track_hash_sum=False, hash_sum_response_key=None,
                 hash_sum_table=None, hash_sum_extractor_impl=None, direct_write=None):
        self.date_manager_mode = date_manager_mode
        self.data_manager_window = data_manager_window
        self.hash_sum_checker_impl = hash_sum_checker_impl
        self.track_hash_sum = track_hash_sum
        self.hash_sum_response_key = hash_sum_response_key
        self.hash_sum_table = hash_sum_table
        self.hash_sum_extractor_impl = hash_sum_extractor_impl
        self.direct_write = direct_write


class DataSourceConfig:

    def __init__(self, data_source=None, data_response_key=None,
                 extra_params=None, api_url=None, limiter=None):
        self.data_source = data_source
        self.data_response_key = data_response_key
        self.extra_params = extra_params
        self.api_url = api_url
        self.limiter = limiter
