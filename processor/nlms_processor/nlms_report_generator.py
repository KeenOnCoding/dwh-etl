"""
This is the first stage
at which the generation of reports is triggered,
the execution status is checked, the report_id is obtained,
then the path to the files on the local machine
is replaced by a http url.
"""
import ast
import time
import requests
from commons.logging.simple_logger import logger


class NlmsReports:
    """
    This class has methods for triggering the generation of reports,
    checking the generation status, taking the report_id
    by which to get the file names of all reports,
    replacing the path on the local machine with
    a http url for further processing.
    """
    def __init__(self,
                 trigger_url,
                 check_status_url,
                 on_prem_report_path,
                 http_report_url,
                 header_key):
        self.trigger_url = trigger_url
        self.check_status_url = check_status_url
        self.on_prem_report_path = on_prem_report_path
        self.http_report_url = http_report_url
        self.header_key = header_key

    def generate_report(self) -> list:
        """
        In this method report generation is triggered.
        The result is a dict like string containing
        "task_id" key with value.
        """
        try:

            report = requests.get(url=self.trigger_url,
                                  headers={'X-Edx-Api-Key': self.header_key})
            logger.info("Triggered report generating...")

            report_id = self.get_report_id(report=report)
            logger.info(f"Generated report with ID: {report_id}")

            report_status = self.check_report_status(report_id=report_id,
                                                     header_key=self.header_key)

            reports_urls = self.replace_reports_path_with_url(reports=report_status,
                                                              on_prem_path=self.on_prem_report_path,
                                                              http_get_link=self.http_report_url)
            logger.info("Replaced reports paths by url")

            return reports_urls
        except KeyError as error:
            logger.info(f"{error}")
            return []

    @staticmethod
    def get_report_id(report) -> str:
        """
        This method takes the result from generate_report()
        and returns string which contain "task_id" value.
        """
        task_id = ast.literal_eval(report.text)

        return task_id['task_id']

    def check_report_status(self, report_id, header_key) -> list:
        """
        This method takes as input report_id,
        checks the status of the report until it is "SUCCESS".
        If the status is SUCCESS, a list of report file paths is returned.
        """
        while True:

            check_request = requests.get(
                url=f'{self.check_status_url}{report_id}',
                headers={'X-Edx-Api-Key': header_key})

            report_status = ast.literal_eval(check_request.text)

            if report_status['status'] != "SUCCESS":
                logger.info(f"Report Status: {report_status}. Waiting for status change 60sec. ")
                time.sleep(60)
            else:
                logger.info(f"Report Status: {report_status}.")
                return report_status['reports']

    @staticmethod
    def replace_reports_path_with_url(reports, on_prem_path, http_get_link) -> list:
        """
        This method replaces file path with url
        """

        reports_filenames = [el.replace(on_prem_path, http_get_link) for el in reports]

        return reports_filenames
