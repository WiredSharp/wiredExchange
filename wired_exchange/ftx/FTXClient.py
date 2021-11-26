import hashlib
import hmac
import math
import time
import urllib.parse

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import httpx

from wired_exchange import to_timestamp_in_seconds, ExchangeClient

from typing import Union

RESOLUTION = 60 * 60


def _to_kline(base: str, quote: str, candles: list):
    df = pd.DataFrame(candles)
    df['base_currency'] = base
    df['quote_currency'] = quote
    df['startTime'] = pd.to_datetime(df['startTime'])
    df.set_index('startTime')
    df.astype(dict(open='float', high='float', low='float', close='float', volume='float', time='int',
                   base_currency='string', quote_currency='string'))
    return df


class FTXClient(ExchangeClient):
    """FTX API client"""

    def __init__(self, api_key=None, api_secret=None, subaccount_name=None, host_url=None):
        super().__init__('ftx', api_key, api_secret, host_url)
        self.subaccount_name = subaccount_name

    def _authenticate(self, request):
        ts = int(time.time() * 1000)
        signature_payload = f'{ts}{request.method.upper()}{request.url.raw_path.decode("utf-8")}'
        if request.content:
            signature_payload += request.content.decode('utf-8')
        self._logger.debug(f'timestamp: {ts}')
        self._logger.debug(f'payload: {signature_payload}')
        signature = hmac.new(self._api_secret.encode("utf-8"), signature_payload.encode("utf-8"),
                             hashlib.sha256).hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self.subaccount_name is not None:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self.subaccount_name)
        self._logger.debug('authentication headers added')

    def get_transactions(self, start_time=None, end_time=None):
        self.open()
        params = {}
        if start_time is not None:
            params['start_time'] = to_timestamp_in_seconds(start_time)
        if end_time is not None:
            params['end_time'] = to_timestamp_in_seconds(end_time)
        try:
            request = self._httpClient.build_request('GET', '/fills', params=params)
            response = self._httpClient.send(request).json()
            if not response['success']:
                raise Exception('FTX response is not a success')
            return self._to_transactions(response['result'])
        except httpx.HTTPStatusError as ex:
            raise Exception('cannot retrieve transactions from FTX') from ex

    def get_prices_history(self, base_currency: str, quote_currency: str,
                           start_time: Union[datetime, int, float], end_time: Union[datetime, int, float],
                           resolution: int):
        self.open()
        params = {'start_time': to_timestamp_in_seconds(start_time) if isinstance(start_time, datetime) else int(
            round(start_time)),
                  'end_time': to_timestamp_in_seconds(end_time) if isinstance(end_time, datetime) else int(
                      round(end_time)),
                  'resolution': resolution}
        params['start_time'] -= resolution
        params['end_time'] += resolution
        try:
            request = self._httpClient.build_request('GET', f'/markets/{base_currency}/{quote_currency}/candles',
                                                     params=params)
            response = self._httpClient.send(request).json()
            if not response['success']:
                raise Exception('FTX response is not a success')
            klines = _to_kline(base_currency, quote_currency, response['result'])
            return klines

        except BaseException as ex:
            raise Exception(
                f'cannot retrieve {base_currency}/{quote_currency} price between {start_time} and {end_time} from FTX') from ex

    def resolve_price(self, asof_time: Union[datetime, int, float], base_currency: str, quote_currency: str):
        self.open()
        window_size = 15
        asof_time = asof_time if isinstance(asof_time, datetime) else datetime.fromtimestamp(asof_time)
        params = {'start_time': to_timestamp_in_seconds(asof_time - timedelta(seconds=asof_time.second)
                                                        + timedelta(
            seconds=math.floor(asof_time.second / window_size) * window_size)),
                  'end_time': to_timestamp_in_seconds(asof_time + timedelta(seconds=window_size)),
                  'resolution': window_size}
        try:
            request = self._httpClient.build_request('GET', f'/markets/{base_currency}/{quote_currency}/candles',
                                                     params=params)
            response = self._httpClient.send(request).json()
            if not response['success']:
                raise Exception('FTX response is not a success')
            return response['result'][0]['close']

        except BaseException as ex:
            raise Exception(f'cannot retrieve {base_currency}/{quote_currency} price at {asof_time} from FTX') from ex

    def _find_price(self, symbol: str, prices: pd.DataFrame, asof_date: datetime):
        if symbol == 'USD':
            return 1.0
        result = prices[(prices.startTime < asof_date) & (prices.base_currency == symbol)].tail(1)['close']
        if result.size == 1:
            return result.iat[0]
        else:
            return np.nan

    def enrich_usd_prices(self, tr):
        """retrieve quote and fee currencies usd equivalent"""
        prices = pd.DataFrame()
        for priceRange in pd.DataFrame(tr[tr['fee_currency'] != 'USD'].groupby(['fee_currency']).agg(['min', 'max'])[
                                           'time']).append(
            tr[tr['quote_currency'] != 'USD'].groupby(['quote_currency']).agg(['min', 'max'])[
                'time']).itertuples():
            try:
                prices = prices.append(
                    self.get_prices_history(priceRange.Index, 'USD', priceRange.min, priceRange.max, RESOLUTION))
                self._logger.info(f'USD prices retrieved for {priceRange.Index}')
            except:
                self._logger.error(f'unable to retrieve prices for {priceRange.Index}/USD', exc_info=True)
        tr['price_usd'] = tr.apply(lambda row: self._find_price(row.quote_currency, prices, row.time), axis=1)
        tr['fee_usd'] = tr.apply(lambda row: self._find_price(row.fee_currency, prices, row.time), axis=1)
        return tr, prices

    # {
    #     "id": 5025968617,
    #     "market": "BTC\/USDT",
    #     "future": null,
    #     "baseCurrency": "BTC",
    #     "quoteCurrency": "USDT",
    #     "type": "order",
    #     "side": "buy",
    #     "price": 57882.0,
    #     "size": 0.0006,
    #     "orderId": 97286633245.0,
    #     "time": "2021-11-18T15:25:37.926Z",
    #     "tradeId": 2494150920.0,
    #     "feeRate": 0.000665,
    #     "fee": 0.023094918,
    #     "feeCurrency": "USDT",
    #     "liquidity": "taker",
    #     "platform": "ftx",
    #     "price_usd": 64189.0,
    #     "fee_usd": 1.0005
    # }
    def _to_transactions(self, fills: list):
        tr = pd.DataFrame(fills)
        tr['platform'] = self.platform
        tr['time'] = pd.to_datetime(tr['time'])
        tr.rename(
            columns=dict(baseCurrency='base_currency', quoteCurrency='quote_currency', feeCurrency='fee_currency'), inplace=True)
        tr.drop(['market', 'future', 'liquidity'], axis='columns', inplace=True)
        tr = tr.astype(
            dict(base_currency='string', quote_currency='string', type='string', side='string', fee_currency='string'))
        self.enrich_usd_prices(tr)
        return tr
