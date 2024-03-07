from configurations.utility import is_exist


class ProcessingConfiguration:
    def __init__(self, sql_processor=None, daily_processor=None, history_processor=None,
                 spreadsheets_processor=None, work_experience_processor=None,
                 nlms_report_processor=None, riksbank_crossrate_processor=None):
        self.sql_processor = SQLProcessor(**is_exist(sql_processor))
        self.daily_processor = DailyProcessor(**is_exist(daily_processor))
        self.history_processor = HistoricalProcessor(**is_exist(history_processor))
        self.spreadsheets_processor = SpreadsheetsProcessor(**is_exist(spreadsheets_processor))
        self.work_experience_processor = WorkExperienceProcessor(**is_exist(work_experience_processor))
        self.nlms_report_processor = NlmsReportProcessor(**is_exist(nlms_report_processor))
        self.riksbank_crossrate_processor = RiksbankProcessor(**is_exist(riksbank_crossrate_processor))


class SQLProcessor:
    def __init__(self, plan_id=None, dynamic_variable=None, default_partition_value=0,
                 catchup=True, raise_when_no_batches_to_process=None, tables_to_rewrite=None,
                 min_batches_to_process=1, tmp_tables=None):
        self.plan_id = plan_id
        self.dynamic_variable = dynamic_variable
        self.default_partition_value = default_partition_value
        self.catchup = catchup
        self.raise_when_no_batches_to_process = raise_when_no_batches_to_process
        self.tables_to_rewrite = tables_to_rewrite
        self.min_batches_to_process = min_batches_to_process
        self.tmp_tables = tmp_tables


class DailyProcessor:
    def __init__(self, daily_proc_target_table=None, daily_proc_source_table=None,
                 default_partition_value=None, batch_window=None, date_column=None):
        self.daily_proc_target_table = daily_proc_target_table
        self.daily_proc_source_table = daily_proc_source_table
        self.default_partition_value = default_partition_value
        self.batch_window = batch_window
        self.date_column = date_column


class HistoricalProcessor:
    def __init__(self, history_table=None, staging_table=None,
                 history_req_field_mappings=None, staging_req_field_mappings=None):
        self.history_table = history_table
        self.staging_table = staging_table
        self.history_req_field_mappings = history_req_field_mappings
        self.staging_req_field_mappings = staging_req_field_mappings


class SpreadsheetsProcessor:
    def __init__(self, target_table=None, google_table=None,
                 scope=None, spreadsheet_id=None, range=None):
        self.target_table = target_table
        self.google_table = google_table
        self.scope = scope
        self.spreadsheet_id = spreadsheet_id
        self.range = range


class WorkExperienceProcessor:
    def __init__(self, work_experience_target_table=None, work_experience_source_table=None):
        self.work_experience_target_table = work_experience_target_table
        self.work_experience_source_table = work_experience_source_table


class NlmsReportProcessor:
    def __init__(self, nlms_report_target_table=None,
                 trigger_url=None,
                 check_status_url=None,
                 on_prem_report_path=None,
                 http_report_url=None,
                 header_key=None):
        self.nlms_report_target_table = nlms_report_target_table
        self.trigger_url = trigger_url
        self.check_status_url = check_status_url
        self.on_prem_report_path = on_prem_report_path
        self.http_report_url = http_report_url
        self.header_key = header_key


class RiksbankProcessor:
    def __init__(self,
                 riksbank_currencies_target_table=None,
                 riksbank_currencies_latest_table=None,
                 observation_url=None,
                 crossrate_url=None,
                 currency_codes_source_table=None,
                 default_partition_value=None,
                 sleep_duration=None):
        self.riksbank_currencies_target_table = riksbank_currencies_target_table
        self.riksbank_currencies_latest_table = riksbank_currencies_latest_table
        self.observation_url = observation_url
        self.crossrate_url = crossrate_url
        self.currency_codes_source_table = currency_codes_source_table
        self.default_partition_value = default_partition_value
        self.sleep_duration = sleep_duration
