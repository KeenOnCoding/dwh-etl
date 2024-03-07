from abc import ABC, abstractmethod
from collections import namedtuple

from commons.logging.simple_logger import logger
from ingestion.data_sources.http_data_source import HttpDataSource
from ingestion.exec.incremental_exec_context import SimpleIncrementalExecutionContextBuilder
from ingestion.hash_sum_utils.hash_sum_manager import HashsumRecord

HashSumCheckerResponse = namedtuple('HashSumCheckerResponse',
                                    ['context', 'is_modified'])


class HashSumChecker(ABC):
    def digest(self, payload: HashsumRecord):
        self._prepare_context(payload)
        return self

    @abstractmethod
    def _prepare_context(self, payload: HashsumRecord):
        raise NotImplementedError

    @abstractmethod
    def check(self) -> HashSumCheckerResponse:
        raise NotImplementedError


class OneCHashSumChecker(HttpDataSource, HashSumChecker):

    def __init__(self, config, query_params_builder, auth):
        HttpDataSource.__init__(self, query_params_builder=query_params_builder, auth=auth)

        self.config = config
        self.query_params_builder = query_params_builder

    def _prepare_context(self, payload: HashsumRecord):
        self.context = self.__build_context(payload)
        self.hash_sum = payload.hash_sum

    def __build_context(self, payload: HashsumRecord):
        if self.config.job.ingestion.fetching_strategy.fetching_strategy != 'INCREMENTAL':
            raise RuntimeError('Only INCREMENTAL fetching strategy is '
                               'supported')

        try:
            from datetime import datetime
            from ingestion.exec.batch import BatchIdProvider
            return SimpleIncrementalExecutionContextBuilder.build(
                batch_id=BatchIdProvider.get_batch_id(),
                date_from=payload.date_from,
                date_to=payload.date_to
            )
        except KeyError:
            raise RuntimeError('Found corrupted record with no dateto/datefrom'
                               f'parameters: {payload}')

    def check(self) -> HashSumCheckerResponse:
        return super(HttpDataSource, self).extract_data(self.context,
                                                        HASH_SUM=self.hash_sum)

    def _process_response(self, response, batch_context, **kwargs):
        response_dict = response.json()
        logger.info(f"Status for current version: {response_dict['status']}")
        return HashSumCheckerResponse(batch_context,
                                      response_dict['status'] == 'MODIFIED')

    def _enrich_request(self, request, **kwargs):
        import urllib.parse
        url = request["url"]
        request["url"] = f"{url}&version={urllib.parse.quote_plus(kwargs['HASH_SUM'])}"
        return request


class HashSumCheckerFactory:
    def __init__(self, config, query_params_builder=None, auth=None):
        self.config = config
        self.query_params_builder = query_params_builder
        self.auth = auth

    def create(self):
        checker_impl = self.config.job.ingestion.data_manager.hash_sum_checker_impl
        logger.info(f"Using {checker_impl} hash sum checker strategy")

        if checker_impl == '1C':
            checker = OneCHashSumChecker(self.config,
                                         self.query_params_builder,
                                         auth=self.auth)
        else:
            raise RuntimeError("HASH_SUM_CHECKER_IMPL is not correct")

        return checker
