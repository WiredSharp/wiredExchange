import logging
import os

from datetime import datetime

import httpx
import pandas as pd

from wired_exchange.core import VERSION, config

from typing import Union


def raise_on_4xx_5xx(response):
    response.raise_for_status()


class ExchangeClient:
    def __init__(self, platform: str, api_key: str = None, api_secret: str = None,
                 host_url: str = None, always_authenticate: bool = True):
        self._logger = logging.getLogger(type(self).__name__)
        self.platform = platform
        self._httpClient = None
        self.always_authenticate = always_authenticate
        self._api_key = api_key if api_key is not None else self._get_exchange_env_value('api_key')
        self._api_secret = api_secret if api_secret is not None else self._get_exchange_env_value('api_secret')
        self.host_url = host_url if host_url is not None else self._get_exchange_config()['url']

    def _get_exchange_env_value(self, key: str):
        return os.getenv(f'{self.platform}_{key}')

    def _get_exchange_config(self):
        return config()['exchanges'][self.platform]

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self._httpClient is None:
            self._httpClient = httpx.Client(base_url=self.host_url,
                                            event_hooks={
                                                'request': [self._log_request,
                                                            self._authenticate] if self.always_authenticate else [
                                                    self._log_request],
                                                'response': [self._log_response, raise_on_4xx_5xx]},
                                            headers={'Accept': 'application/json',
                                                     "User-Agent": "wired_exchange/" + VERSION})
            self._logger.debug(f'instantiate http client for {self}')
        return self

    def close(self):
        if self._httpClient is not None:
            self._httpClient.close()
            self._logger.debug('close http client')
        self._httpClient = None

    def _authenticate(self, request: httpx.Request):
        raise NotImplementedError('Httpx event hook to be implemented in derived classes')

    def __str__(self):
        return f'{self.platform} exchange client'

    def _log_request(self, request):
        self._logger.debug(request.headers)
        self._logger.debug(f"Request event hook: {request.method} {request.url} - Waiting for response")

    def _log_response(self, response):
        request = response.request
        self._logger.debug(f"Response event hook: {request.method} {request.url} - Status {response.status_code}")

    def get_prices_history(self, base_currency: str, quote_currency: str, resolution: int,
                           start_time: Union[datetime, int, float],
                           end_time: Union[datetime, int, float, type(None)] = None) -> pd.DataFrame:
        raise NotImplementedError('to be implemented in derived class')
