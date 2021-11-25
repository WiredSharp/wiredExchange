import base64
import hashlib
import hmac
import time
from typing import Literal
from pandas import DataFrame

import httpx
from wired_exchange import to_timestamp_in_milliseconds, ExchangeClient


class KucoinClient(ExchangeClient):

    def __init__(self, api_key: str = None, api_passphrase: str = None,
                 api_secret: str = None, host_url: str = None):
        super().__init__('kucoin', api_key, api_secret, host_url, always_authenticate=False)
        self._api_passphrase = api_passphrase if api_passphrase is not None else self._get_exchange_env_value(
            'api_passphrase')

    def _authenticate(self, request):
        ts = int(round(time.time())) * 1000
        self._logger.debug(f'timestamp {ts}')
        signature_payload = f'{str(ts)}{request.method.upper()}{request.url.raw_path.decode("utf-8")}'
        if request.content:
            signature_payload += request.content.decode('utf-8')
        signature = base64.b64encode(
            hmac.new(self._api_secret.encode('utf-8'), signature_payload.encode('utf-8'), hashlib.sha256).digest())
        self._logger.debug(f'payload {signature_payload}')
        passphrase = base64.b64encode(
            hmac.new(self._api_secret.encode('utf-8'), self._api_passphrase.encode('utf-8'), hashlib.sha256).digest())
        request.headers['KC-API-SIGN'] = signature.decode('utf-8')
        request.headers['KC-API-TIMESTAMP'] = str(ts)
        request.headers['KC-API-KEY'] = self._api_key
        request.headers['KC-API-PASSPHRASE'] = passphrase.decode('utf-8')
        request.headers['KC-API-KEY-VERSION'] = "2"

    def get_transactions(self, start_time=None, end_time=None, trade_type: Literal['spot', 'margin'] = 'spot'):
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        if start_time is not None:
            params['startAt'] = to_timestamp_in_milliseconds(start_time)
        if end_time is not None:
            params['endAt'] = to_timestamp_in_milliseconds(end_time)
        try:
            request = self._httpClient.build_request('GET', '/v1/fills', params=params)
            self._authenticate(request)
            response = self._httpClient.send(request)
            return self.to_trade(response.json())
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def get_orders(self, start_time=None, end_time=None, trade_type: Literal['spot', 'margin'] = 'spot',
                   status: Literal['done', 'active'] = None):
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        if status is not None:
            params['status'] = status
        if start_time is not None:
            params['startAt'] = to_timestamp_in_milliseconds(start_time)
        if end_time is not None:
            params['endAt'] = to_timestamp_in_milliseconds(end_time)
        try:
            request = self._httpClient.build_request('GET', '/v1/orders', params=params)
            self._authenticate(request)
            response = self._httpClient.send(request)
            response.json()
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def to_trade(self, fills: dict):
        df = DataFrame(fills['items']).rename(columns={'tradeid': 'id'}).set_index('id')
        df['platform']
        return {}