import base64
import datetime
import json
import requests

from abc import ABC, abstractmethod
from commons.logging.simple_logger import logger


class AuthorizationStrategy(ABC):
    @abstractmethod
    def authorize(self, request):
        pass


class BaseAuthStrategy(AuthorizationStrategy):
    def authorize(self, request):
        return request


class OAuth2Strategy(AuthorizationStrategy):
    def __init__(self, client_id=None, client_secret=None, url=None, scope=None, **kwargs):
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = url
        self.scope = scope
        self.kwargs = kwargs
        self.token = None
        self.valid_until = datetime.datetime.now()

    def authorize(self, request):
        request['headers']['Authorization'] = self.get_token()
        return request

    def get_token(self):
        if not self.token or self.valid_until < datetime.datetime.now():
            data = {'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    }

            if self.scope is not None:
                data['scope'] = self.scope

            try:
                response = requests.post(url=self.url, data=data, verify=False, **self.kwargs)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.error('HTTP request failed! '
                             f'Response status: {response.text}')
                raise e
            logger.info(f"Token was generated, response status: {response.status_code}")
            request_time = datetime.datetime.now()

            json_response = json.loads(response.text)
            expiration_time = json_response["expires_in"]

            self.valid_until = request_time + datetime.timedelta(seconds=expiration_time)

            access_token = json_response['access_token']
            token_type = json_response['token_type']
            self.token = f"{token_type} {access_token}"

        return self.token


class BasicAuthStrategy(AuthorizationStrategy):
    def __init__(self, user, password):
        self.user = user
        self.password = password

    def authorize(self, request):
        encoded_credentials = self.encode(f"{self.user}:{self.password}")
        request['headers']['Authorization'] = f"Basic {encoded_credentials}"
        return request

    def encode(self, data):
        return str(base64.b64encode(data.encode("utf-8")), "utf-8")
