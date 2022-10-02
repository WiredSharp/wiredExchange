from datetime import date
import time
from typing import Union, List

import httpx

from wired_exchange.core.ExchangeClient import ExchangeClient

MAX_RETRY = 7

REQUEST_DELAY = 3


class ExchangeRatesClient(ExchangeClient):
    """AbstractApi Exchange rates API client
        see https://app.abstractapi.com/api/exchange-rates/documentation"""

    def __init__(self, api_secret=None, host_url=None):
        super().__init__('exchange_rates', None, api_secret, host_url, always_authenticate=False)

    def get_live_rate(self, base: str, quote: Union[str, List[str]]) -> float:
        self.open()
        param = {'api_key': self._api_key, 'base': base}
        if isinstance(quote, str):
            param['target'] = quote
        else:
            param['target'] = ','.join(quote)
        try:
            response = self._send_get('/v1/live/', param)
            return response['exchange_rates']
        except BaseException as ex:
            raise Exception(f'cannot retrieve live {base}/{quote} rates from AbstractApi') from ex

    def get_rate(self, base: str, quote: Union[str, List[str]], quote_date: date) -> float:
        self.open()
        param = {'api_key': self._api_key, 'base': base, 'date': quote_date.strftime('%Y-%m-%d')}
        if isinstance(quote, str):
            param['target'] = quote
        else:
            param['target'] = ','.join(quote)
        try:
            response = self._send_get('/v1/historical/', param)
            return response['exchange_rates']
        except BaseException as ex:
            raise Exception(f'cannot retrieve historical {base}/{quote} rates from AbstractApi') from ex

    def _send_get(self, path: str, params: dict = None):
        request = self._httpClient.build_request('GET', path, params=params)
        retry = 0
        while retry < MAX_RETRY:
            try:
                response = self._httpClient.send(request)
                return response.json()
            except httpx.HTTPStatusError as ex:
                if 429 == ex.response.status_code:
                    retry += 1
                    self._logger.warning(f'request threshold reach, waiting {REQUEST_DELAY}s #{retry}...')
                    time.sleep(REQUEST_DELAY)
                else:
                    raise ex
        raise Exception(f'too many requests, cannot get response from {path}')
