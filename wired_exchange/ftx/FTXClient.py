import hmac
import logging
import os
import time
import urllib.parse

import httpx

from wired_exchange import _config, _log_request, log_response, raise_on_4xx_5xx, to_timestamp_in_seconds


class FTXClient:
    def __init__(self, api_key=None, api_secret=None, subaccount_name=None, host_url=None):
        self.__api_key = api_key if api_key is not None else os.getenv('ftx_api_key')
        self.__api_secret = api_secret if api_secret is not None else os.getenv('ftx_api_secret')
        self.subaccount_name = subaccount_name
        self.__logger = logging.getLogger(type(self).__name__)
        self.host_url = host_url if host_url is not None else _config['exchanges']['ftx']['url']
        self.__httpClient = None

    def __authenticate(self, request):
        ts = int(time.time() * 1000)
        signature_payload = f'{ts}{request.method.upper()}{request.url.raw_path.decode()}'
        if request.content:
            signature_payload += request.content
        self.__logger.debug(f'timestamp {ts}')
        self.__logger.debug(f'payload {signature_payload}')
        signature = hmac.new(self.__api_secret.encode(), signature_payload.encode(), 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self.__api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self.subaccount_name is not None:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self.subaccount_name)
        self.__logger.debug('authentication headers added')

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self.__httpClient is None:
            self.__httpClient = httpx.Client(base_url=self.host_url,
                                             event_hooks={
                                                 'request': [self.__authenticate, _log_request],
                                                 'response': [log_response, raise_on_4xx_5xx]},
                                             headers={'Accept': 'application/json'})
            self.__logger.debug('instantiate http client')
        return self

    def close(self):
        if self.__httpClient is not None:
            self.__httpClient.close()
            self.__logger.debug('close http client')
        self.__httpClient = None

    def get_transactions(self, start_time=None, end_time=None):
        self.open()
        params = {}
        if start_time is not None:
            params['start_time'] = to_timestamp_in_seconds(start_time)
        if end_time is not None:
            params['end_time'] = to_timestamp_in_seconds(end_time)
        try:
            request = self.__httpClient.build_request('GET', '/fills', params=params)
            response = self.__httpClient.send(request)
            return response.json()
        except httpx.HTTPStatusError as ex:
            self.__logger.error('cannot retrieve transactions from FTX', ex)