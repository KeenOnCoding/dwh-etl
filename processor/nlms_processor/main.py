from nlms_report_generator import NlmsReports
from stage_table_processor import StageTableProcessor


class NlmsReportRunner:

    def __init__(self, trigger_url, check_status_url, on_prem_report_path,
                 http_report_url, spark_session, target_table, spark_db_sink, header_key):
        self.trigger_url = trigger_url
        self.check_status_url = check_status_url
        self.on_prem_report_path = on_prem_report_path
        self.http_report_url = http_report_url
        self.spark_session = spark_session
        self.target_table = target_table
        self.spark_db_sink = spark_db_sink
        self.header_key = header_key

    def run(self) -> None:

        # generate reports
        nlms_reports = NlmsReports(trigger_url=self.trigger_url,
                                   check_status_url=self.check_status_url,
                                   on_prem_report_path=self.on_prem_report_path,
                                   http_report_url=self.http_report_url,
                                   header_key=self.header_key) \
            .generate_report()

        stage_table = StageTableProcessor(spark_session=self.spark_session,
                                          nlms_reports=nlms_reports) \
            .create_stage_table()

        try:
            self.spark_db_sink.save(data=stage_table, table_name=self.target_table)
        except RuntimeError as error:
            print(f"{error}")
