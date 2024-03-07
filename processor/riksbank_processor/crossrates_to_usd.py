"""
CROSSRATE SEK{currency} TO SEKUSD
"""
import ast
import time
import requests


class CrossRateToUsd:

    def __init__(self,
                 obs_url,
                 cross_url,
                 date_from,
                 keys,
                 sleep_duration):
        self.obs_url = obs_url
        self.cross_url = cross_url
        self.date_from = date_from
        self.keys = keys
        self.sleep_duration = sleep_duration

    def prepare_data_to_schema(self) -> list:

        output_data = []

        for key in self.keys:
            try:
                # for SEK we use OBSERVATION
                if key == 'SEK':
                    request_data = self.make_observation_request(key=key)
                # for OTHER CURRENCIES we use CrossRates
                elif key:
                    request_data = self.make_crossrate_request(key=key)
                # add {'currency': 'code'} in every record
                for element in request_data:
                    element['currency'] = key
                    # trap for INT  values because
                    # an int raises a ValueError when pySpark creates df
                    element['value'] = float(element['value'])
                    output_data.append(element)

                time.sleep(self.sleep_duration)

            except (SyntaxError, ValueError, TypeError) as error:
                # fix for situations when we get a 204 error upon request OR
                # when the currency has the parameter "series_closed" False,
                # but stopped trading for certain reasons
                # (for example, RUB - the currency is not closed,
                # unfortunately, but there is no data since 2023-03-25)
                print(f'No data for {key}. {error}')
                currency_dict = {'currency': f'{key}', 'value': None}
                output_data.append(currency_dict)
                time.sleep(self.sleep_duration)

        return output_data

    def make_crossrate_request(self, key: str):
        """
        date: format yyyy-mm-dd and type -> str
        """
        try:
            rate = requests.get(
                f'{self.cross_url}/SEK{key}PMI/SEKUSDPMI/{self.date_from}')
            data = rate.json()
            return data

        except (SyntaxError, ValueError) as error:
            print(error)
            return None

    def make_observation_request(self, key: str):
        """
        date: format yyyy-mm-dd and type -> str
        """
        try:
            rate = requests.get(f'{self.obs_url}/{key}USDPMI/{self.date_from}')
            data = rate.json()
            return data

        except (SyntaxError, ValueError) as error:
            print(error)
            return None
