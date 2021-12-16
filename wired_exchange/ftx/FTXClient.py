import hashlib
import hmac
import math
import time
import urllib.parse

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import httpx

from wired_exchange.core import to_timestamp_in_seconds, to_klines, to_transactions, to_timestamp
from wired_exchange.core.ExchangeClient import ExchangeClient

from typing import Union

RESOLUTION = 60 * 60


def _to_klines(base: str, quote: str, candles: list):
    df = pd.DataFrame(candles)
    if df.size == 0:
        return df
    df['base_currency'] = base
    df['quote_currency'] = quote
    df.drop(['startTime'], axis='columns', inplace=True)
    return to_klines(df, base, quote)


def _find_price(symbol: str, prices: pd.DataFrame, asof_date: datetime):
    if symbol == 'USD':
        return 1.0
    result = prices[(prices.index < asof_date) & (prices.base_currency == symbol)].tail(1)['close']
    if result.size == 1:
        return result.iat[0]
    else:
        return np.nan


class FTXClient(ExchangeClient):
    """FTX API client"""

    def __init__(self, api_key=None, api_secret=None, subaccount_name=None, host_url=None):
        super().__init__('ftx', api_key, api_secret, host_url, always_authenticate=False)
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
        self._add_date_range_params(params, start_time, end_time, precision='s')
        try:
            response = self._send_get('/fills', params, authenticated=True)
            return self._to_transactions(response['result'])
        except httpx.HTTPStatusError as ex:
            raise Exception('cannot retrieve transactions from FTX') from ex

    def get_prices_history(self, base_currency: str, quote_currency: str, resolution: int,
                           start_time: Union[datetime, int, float], end_time: Union[datetime, int, float, None] = None):
        self.open()
        params = {'resolution': resolution}
        self._add_date_range_params(params, start_time, end_time, precision='s')
        params['start_time'] -= resolution
        params['end_time'] += resolution
        try:
            response = self._send_get(f'/markets/{base_currency}/{quote_currency}/candles', params=params)
            klines = _to_klines(base_currency, quote_currency, response['result'])
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
            response = self._send_get(f'/markets/{base_currency}/{quote_currency}/candles', params=params)
            if len(response['result']) > 0:
                return response['result'][0]['close']
            else:
                return np.nan

        except BaseException as ex:
            raise Exception(f'cannot retrieve {base_currency}/{quote_currency} price at {asof_time} from FTX') from ex

    def resolve_current_price(self, base_currency: str, quote_currency: str):
        self.open()
        try:
            response = self._send_get(f'/markets/{base_currency}/{quote_currency}')
            if len(response['result']) > 0:
                return response['result']['price']
            else:
                return np.nan
        except BaseException as ex:
            raise Exception(f'cannot retrieve {base_currency}/{quote_currency} current price from FTX') from ex

    def enrich_usd_prices(self, tr):
        """retrieve quote and fee currencies usd equivalent"""
        prices = None
        if tr.size == 0:
            return tr, tr
        for priceRange in pd.DataFrame(tr[tr['fee_currency'] != 'USD'].groupby(['fee_currency']).agg(['min', 'max'])[
                                           'time']).append(
            tr[tr['quote_currency'] != 'USD'].groupby(['quote_currency']).agg(['min', 'max'])[
                'time']).itertuples():
            try:
                new_prices = self.get_prices_history(priceRange.Index, 'USD', RESOLUTION, priceRange.min,
                                                     priceRange.max)
                if prices is None:
                    prices = new_prices
                else:
                    prices = prices.append(new_prices)
                self._logger.info(f'USD prices retrieved for {priceRange.Index}')
            except:
                self._logger.error(f'unable to retrieve prices for {priceRange.Index}/USD', exc_info=True)
        tr['price_usd'] = tr.apply(lambda row: _find_price(row.quote_currency, prices, row.time), axis='columns')
        tr['fee_usd'] = tr.apply(lambda row: _find_price(row.fee_currency, prices, row.time), axis='columns')
        return tr, prices

    def get_orders(self, start_time: Union[datetime, int, float, None] = None,
                   end_time: Union[datetime, int, float, None] = None):
        self.open()
        params = {}
        self._add_date_range_params(params, start_time, end_time, precision='s')
        try:
            response = self._send_get(f'/orders/history', params=params, authenticated=True)
            return self._to_orders(response['result'])

        except BaseException as ex:
            raise Exception(
                'cannot retrieve orders from FTX') from ex

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
        if tr.size == 0:
            return tr
        tr['platform'] = self.platform
        tr['time'] = pd.to_datetime(tr['time'])
        tr.rename(
            columns=dict(baseCurrency='base_currency', quoteCurrency='quote_currency',
                         orderId='order_id', tradeId='trade_id', feeCurrency='fee_currency',
                         feeRate='fee_rate', id='fill_id'),
            inplace=True)
        tr.astype(dict(order_id='string', trade_id='string', fill_id='string'))
        tr['id'] = tr['fill_id'].apply(lambda id: f'{self.platform}_{id}')
        tr.drop(['market', 'future', 'liquidity', 'fill_id'], axis='columns', inplace=True)
        tr = to_transactions(tr)
        self.enrich_usd_prices(tr)
        return tr

    def get_balances(self):
        self.open()
        try:
            response = self._send_get('/wallet/balances', authenticated=True)
            return self._to_balances(response['result'])

        except BaseException as ex:
            raise Exception(
                f'cannot retrieve positions from FTX') from ex

    #  {
    #     "coin": "ETH",
    #     "total": 0.0899829,
    #     "free": 0.3212904,
    #     "availableWithoutBorrow": 0.0899829,
    #     "usdValue": 399.7430372727522,
    #     "spotBorrow": 0.0
    #   }
    def _to_balances(self, balances_json: dict):
        balances = pd.DataFrame(balances_json)
        if balances.size == 0:
            return balances
        balances = balances[balances['total'] > 0]
        balances.rename(columns=dict(availableWithoutBorrow='available', coin='currency'), inplace=True)
        balances.drop(columns=['free', 'usdValue', 'spotBorrow'], inplace=True)
        try:
            request = self._httpClient.build_request('GET', '/markets')
            tickers = self._httpClient.send(request).json()
            if not tickers['success']:
                raise Exception('FTX response is not a success')
            tickers = pd.DataFrame(tickers['result'])
            tickers = tickers[tickers['quoteCurrency'] == 'USDT']
            balances = balances.merge(tickers, left_on='currency',
                                      right_on='baseCurrency', how='left')
            balances.drop(columns=['name', 'enabled', 'postOnly', 'restricted',
                                   'highLeverageFeeExempt', 'baseCurrency', 'quoteCurrency',
                                   'underlying', 'type', 'changeBod', 'tokenizedEquity'], inplace=True)
            balances.rename(columns=dict(baseCurrency='currency'), inplace=True)
        except:
            self._logger.warning('cannot retrieve current tickers', exc_info=True)
        balances.set_index('currency', inplace=True)
        balances['platform'] = self.platform
        return balances

    def get_account_operations(self, start_time=None, end_time=None) -> pd.DataFrame:
        self.open()
        params = {}
        self._add_date_range_params(params, start_time, end_time, 's')
        try:
            columns = ['size', 'coin',
                       'status', 'time', 'txid']
            result = self._send_get('/wallet/deposits', params, authenticated=True)
            deposits = pd.DataFrame(result['result'], columns=columns)
            deposits['type'] = 'deposit'
            result = self._send_get('/wallet/withdrawals', params, authenticated=True)
            withdrawals = pd.DataFrame(result['result'], columns=columns)
            withdrawals['type'] = 'withdrawal'
            return self._to_account_operations(deposits, withdrawals)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve account operations from FTX') from ex

    def _send_get(self, path: str, params: dict = None, authenticated: bool = False):
        request = self._httpClient.build_request('GET', path, params=params)
        if authenticated:
            self._authenticate(request)
        response = self._httpClient.send(request).json()
        if not response['success']:
            raise Exception('FTX response is not a success')
        return response

    def _to_account_operations(self, deposits, withdrawals):
        operations = deposits.append(withdrawals, ignore_index=True)
        operations['time'] = pd.to_datetime(operations['time'])
        operations['platform'] = self.platform
        operations.rename(columns=dict(coin='base_currency', txid='id'), inplace=True)
        return operations

    def _to_orders(self, orders: dict):
        frame = pd.DataFrame(orders)
        frame.loc[:, 'base_currency'] = frame['market'].apply(lambda s: s.split('/')[0])
        frame.loc[:, 'quote_currency'] = frame['market'].apply(lambda s: s.split('/')[1])
        frame['time'] = pd.to_datetime(frame['createdAt'])
        frame = frame.loc[:, ('id', 'base_currency', 'quote_currency', 'type', 'side', 'price',
                              'size', 'status', 'time')]
        frame['platform'] = self.platform
        frame.astype(dict(price='float', size='float'))
        return frame

    @staticmethod
    def _add_date_range_params(params: dict, start_time, end_time, precision) -> dict:
        if start_time is not None:
            params['start_time'] = to_timestamp(start_time, precision) if isinstance(start_time, datetime) else int(
                round(start_time))
        if end_time is not None:
            params['end_time'] = to_timestamp(end_time, precision) if isinstance(end_time, datetime) else int(
                round(end_time))
        return params
