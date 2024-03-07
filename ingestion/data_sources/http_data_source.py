import tempfile
import requests

from commons.logging.simple_logger import logger
from ingestion.data_sources.datasource import AbstractDataSource


class HttpDataSource(AbstractDataSource):
    def __init__(self, query_params_builder=None, config=None, hashsum_checker_data_source=None,
                 hash_sum_extractor=None, spark=None, auth=None):
        self.query_params_builder = query_params_builder
        self.config = config
        self.hash_sum_extractor = hash_sum_extractor
        self.spark = spark
        self._hashsum_checker_data_source = hashsum_checker_data_source
        self.auth = auth

    def _enrich_request(self, request, **kwargs):
        return request

    def _extract_data_from_source_system(self, batch_context, **kwargs):
        request = self.auth.authorize(
            self._enrich_request(
                {
                    "url": self.get_request_string(batch_context=batch_context, **kwargs),
                    "headers": self._update_headers({'Accept': 'application/json'}),
                    "verify": False
                },
                **kwargs
            )
        )
        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error('HTTP request failed! '
                         f'Response status: {response.text}')
            raise e
        logger.info(f"Response status: {response.status_code}")
        return response

    def _read_data(self, response):
        with tempfile.NamedTemporaryFile() as tmp:
            for b in response.iter_content(chunk_size=1024):
                tmp.write(b)
            tmp.flush()
            response_df = super().read_json(tmp.name, self.spark).persist()
            logger.info(f"Got response with {response_df.count()} records")
        return response_df

    def _pre_process(self, response, batch_context, **kwargs):
        if self.config.job.ingestion.data_manager.track_hash_sum:
            logger.info(f"Using TRACK_HASH_SUM: '{self.config.job.ingestion.data_manager.track_hash_sum}'")
            super().save_data_hashsum(response, batch_context,
                                      self.hash_sum_extractor,
                                      **kwargs)

    def _post_process(self, response_df):
        if self.config.job.ingestion.data_source.data_response_key is not None:
            response_df = super().nested_data_explode(
                self.spark, response_df,
                self.config.job.ingestion.data_source.data_response_key
            )
        logger.info("Creating dataframe from response")
        return response_df

    def _update_headers(self, headers):
        return headers

    def get_raw_parameters(self, batch_context=None, **kwargs):
        if batch_context is None:
            raise RuntimeError('None batch context was passed.')
        return self.query_params_builder.build(batch_context)

    def get_request_parameters(self, batch_context=None, **kwargs):
        if batch_context is None:
            raise RuntimeError('None batch context was passed.')

        raw_params = self.get_raw_parameters(batch_context, **kwargs)
        extra_params = self.config.job.ingestion.data_source.extra_params

        if extra_params is None:
            params = raw_params
        elif extra_params and raw_params == '':
            params = extra_params
        else:
            params = '&'.join((extra_params, raw_params))
        return params

    def get_request_string(self, batch_context=None, **kwargs):
        _api_url = self.config.job.ingestion.data_source.api_url
        logger.info(f"Making request to url: {_api_url}")
        return "{}?{}".format(_api_url,
                              self.get_request_parameters(
                                  batch_context=batch_context,
                                  **kwargs))
