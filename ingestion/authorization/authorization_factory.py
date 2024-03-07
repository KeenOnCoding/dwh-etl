import constants
from authorization.authorization_strategy import OAuth2Strategy, BaseAuthStrategy, BasicAuthStrategy
from commons.logging.simple_logger import logger


class AuthorizationFactory:
    def __init__(self, config,  **kwargs):
        self.config = config
        self.client_id = config.job.ingestion.auth.client_id
        self.client_secret = config.job.ingestion.auth.client_secret
        self.user = config.job.ingestion.auth.api_user
        self.password = config.job.ingestion.auth.api_password
        self.url = config.job.ingestion.auth.auth_url
        self.scope = config.job.ingestion.auth.scope
        self.kwargs = kwargs

    def create(self):
        auth_strategy = self.config.job.ingestion.auth.auth_strategy
        logger.info(f"Using {auth_strategy} authorization strategy")

        if auth_strategy == constants.OAUTH2_AUTH_STRATEGY:
            auth = OAuth2Strategy(
                client_id=self.client_id,
                client_secret=self.client_secret,
                url=self.url,
                scope=self.scope
            )
        elif auth_strategy == constants.BASIC_AUTH_STRATEGY:
            auth = BasicAuthStrategy(
                user=self.user,
                password=self.password
            )
        else:
            auth = BaseAuthStrategy()
        return auth
