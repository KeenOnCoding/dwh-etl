import json

from commons.logging.simple_logger import logger
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class Spreadsheet:
    def __init__(self, credentials, scopes,
                 spreadsheet_id, range) -> None:

        self.table_data = None
        self.credentials = credentials
        self.scope = scopes
        self.spreadsheet_id = spreadsheet_id
        self.range = range

    def get_data(self) -> list:
        if self.table_data is None:
            self.table_data = self._execute_request()["values"]
            logger.info("Data was extracted")
        return self.table_data

    def _execute_request(self) -> dict:
        service = build('sheets', 'v4',
                        credentials=self._configure_credentials())
        logger.info("Request execution")
        logger.info(f"Using spreadsheets '{self.spreadsheet_id}' and range '{self.range}'")
        return service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range,
            majorDimension="ROWS"
        ).execute()

    def _configure_credentials(self) -> Credentials:
        logger.info("Credentials configuring")
        return Credentials.from_service_account_info(
            json.loads(self.credentials, strict=False),
            scopes=self._get_scopes())

    def _get_scopes(self) -> list:
        logger.info(f"Using scope: '{self.scope}'")
        return [self.scope]
