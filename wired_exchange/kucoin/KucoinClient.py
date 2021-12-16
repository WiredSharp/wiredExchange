import asyncio
import base64
import functools
import hashlib
import hmac
import time
from datetime import datetime, timedelta
from typing import Literal, Union

import httpx
import numpy as np
import pandas as pd
import tzlocal
from pandas import DataFrame

from wired_exchange.core import to_timestamp, to_transactions, to_klines
from wired_exchange.core.ExchangeClient import ExchangeClient
from wired_exchange.kucoin import CandleStickResolution
from wired_exchange.kucoin.WebSocket import KucoinWebSocket


class KucoinClient(ExchangeClient):

    def __init__(self, api_key: str = None, api_passphrase: str = None,
                 api_secret: str = None, host_url: str = None):
        super().__init__('kucoin', api_key, api_secret, host_url, always_authenticate=False)
        self._api_passphrase = api_passphrase if api_passphrase is not None else self._get_exchange_env_value(
            'api_passphrase')
        self._ws = None

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

    def get_transactions(self, start_time, end_time=None,
                         trade_type: Literal['spot', 'margin'] = 'spot') -> pd.DataFrame:
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        self._add_date_range_params(params, start_time, end_time, 'ms')
        try:
            request = self._httpClient.build_request('GET', '/v1/fills', params=params)
            self._authenticate(request)
            response = self._read_pages('/v1/fills', params, authenticated=True)
            return self._to_transactions(response)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve transactions from Kucoin') from ex

    def get_orders(self, symbol: str = None, start_time: Union[datetime, int, float, type(None)] = None,
                   end_time: Union[datetime, int, float, type(None)] = None,
                   trade_type: Literal['spot', 'margin'] = 'spot',
                   status: Literal['done', 'active'] = None,
                   side: Literal['buy', 'sell'] = None) -> pd.DataFrame:
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        if symbol is not None:
            params['symbol'] = symbol
        if status is not None:
            params['status'] = status
        if side is not None:
            params['side'] = side
        try:
            last_query = False
            orders = None
            while not last_query:
                if start_time is not None and end_time is None:
                    end_time = start_time + timedelta(days=7)
                    last_query = end_time >= datetime.now(tzlocal.get_localzone())
                    self._add_date_range_params(params, start_time, end_time, 'ms')
                    start_time = end_time
                    end_time = None
                else:
                    last_query = True
                    self._add_date_range_params(params, start_time, end_time, 'ms')
                current_orders = self._read_pages('/v1/orders', params=params, authenticated=True)
                if len(current_orders) > 0:
                    if orders is None:
                        orders = pd.DataFrame(current_orders)
                    else:
                        orders = orders.append(pd.DataFrame(current_orders), ignore_index=True)
                current_orders = self._read_pages('/v1/stop-order', params=params, authenticated=True)
                if len(current_orders) > 0:
                    if orders is None:
                        orders = pd.DataFrame(current_orders)
                    else:
                        orders = orders.append(pd.DataFrame(current_orders), ignore_index=True)
            if orders is not None:
                if 'cancelExist' in orders.columns:
                    orders = orders[orders['cancelExist'] != True]
                return self._to_orders(orders)
            else:
                return pd.DataFrame()
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve transactions from Kucoin') from ex

    def get_prices_history(self, base_currency: str, quote_currency: str, resolution: int,
                           start_time: Union[datetime, int, float],
                           end_time: Union[datetime, int, float, type(None)] = None) -> pd.DataFrame:
        self.open()
        # For each query, the system would return at most **1500** pieces of data. To obtain more data, please page
        # the data by time.
        params = dict(symbol=f'{base_currency}-{quote_currency}',
                      type=CandleStickResolution.from_seconds(resolution).value)
        self._add_date_range_params(params, start_time, end_time, 's')
        try:
            request = self._httpClient.build_request('GET', '/v1/market/candles', params=params)
            response = self._httpClient.send(request).json()
            if not response['code'].startswith('200'):
                raise RuntimeError(f'{response["code"]}: response code does not indicate a success')
            return self._to_klines(response['data'], base_currency, quote_currency)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve transactions from Kucoin') from ex

    # {
    #     "symbol": "CAKE-USDT",
    #     "tradeId": "61913fb6674fa9563b3c76fe",
    #     "orderId": "6190e3cbfe8b030001429310",
    #     "counterOrderId": "61913fb6f4f1fd0001c2e6df",
    #     "side": "buy",
    #     "liquidity": "maker",
    #     "forceTaker": false,
    #     "price": "18.086",
    #     "size": "0.615",
    #     "funds": "11.12289",
    #     "fee": "0.01112289",
    #     "feeRate": "0.001",
    #     "feeCurrency": "USDT",
    #     "stop": "",
    #     "tradeType": "TRADE",
    #     "type": "limit",
    #     "createdAt": 1636908982000
    # }

    def _to_transactions(self, fills: dict) -> pd.DataFrame:
        tr = DataFrame(fills)
        if tr.size == 0:
            return tr
        tr['time'] = pd.to_datetime(tr['createdAt'], unit='ms', utc=True)
        tr['base_currency'] = tr['symbol'].apply(lambda s: s.split('-')[0])
        tr['quote_currency'] = tr['symbol'].apply(lambda s: s.split('-')[1])
        tr.rename(
            columns=dict(feeCurrency='fee_currency', tradeId='trade_id', orderId='order_id',
                         counterOrderId='counter_order_id', feeRate='fee_rate'), inplace=True)
        tr.drop(['createdAt', 'symbol', 'forceTaker', 'stop', 'tradeType',
                 'funds', 'liquidity', 'counter_order_id'],
                axis='columns', inplace=True)
        tr.astype(dict(order_id='string', trade_id='string'))
        tr['platform'] = self.platform
        tr['id'] = tr['trade_id'].apply(lambda id: f'{self.platform}_{id}')
        return to_transactions(tr)

    # [
    #     [
    #         "1545904980",             //Start time of the candle cycle
    #         "0.058",                  //opening price
    #         "0.049",                  //closing price
    #         "0.058",                  //highest price
    #         "0.049",                  //lowest price
    #         "0.018",                  //Transaction volume
    #         "0.000945"                //Transaction amount
    #     ]
    # ]
    def _to_klines(self, candles: dict, base: str, quote: str) -> pd.DataFrame:
        tr = pd.DataFrame(candles, columns=['time', 'open', 'close', 'high', 'low', 'volume', 'amount'])
        if tr.size == 0:
            return tr
        tr.drop(['amount'], inplace=True, axis='columns')
        tr['time'] = pd.to_numeric(tr['time']) * 1000
        return to_klines(tr, base, quote)

    def get_balances(self) -> pd.DataFrame:
        self.open()
        try:
            request = self._httpClient.build_request('GET', '/v1/accounts')
            self._authenticate(request)
            response = self._httpClient.send(request).json()
            if not response['code'].startswith('200'):
                raise RuntimeError(f'{response["code"]}: response code does not indicate a success')
            return self._to_balances(response['data'])
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve accounts list from Kucoin') from ex

    def _to_balances(self, balances_json: dict) -> pd.DataFrame:
        balances = pd.DataFrame(balances_json)
        if balances.size == 0:
            return balances
        balances.drop(columns=['id', 'type', 'holds'], inplace=True)
        balances['balance'] = pd.to_numeric(balances['balance'])
        balances = balances[balances['balance'] != 0.0]
        balances['available'] = pd.to_numeric(balances['available'])
        balances = balances.groupby('currency').sum()
        try:
            tickers = self.get_all_tickers()
            balances = balances.merge(tickers, left_index=True,
                                      right_on='currency', how='left')
            balances.drop(columns=['symbol', 'symbolName'], inplace=True)
        except:
            self._logger.warning('cannot retrieve current tickers', exc_info=True)
        balances.rename(columns=dict(balance='total', averagePrice='price'), inplace=True)
        balances.set_index('currency', inplace=True)
        balances['platform'] = self.platform
        balances.convert_dtypes()
        return balances

    def get_all_tickers(self) -> pd.DataFrame:
        tickers = self._httpClient.get('v1/market/allTickers').json()
        if not tickers['code'].startswith('200'):
            raise RuntimeError(f'{tickers["code"]}: response code does not indicate a success')
        tickers = self._convert_to_ticker(tickers)
        return tickers

    def _convert_to_ticker(self, tickers: dict) -> pd.DataFrame:
        asof_time = pd.to_datetime(tickers['data']['time'], unit='ms', utc=True)
        tickers = pd.DataFrame(tickers['data']['ticker'])
        tickers = tickers[tickers['symbol'].str.endswith('USDT')]
        tickers['currency'] = tickers['symbol'].apply(lambda s: s.split('-')[0])
        tickers['time'] = asof_time
        tickers.rename(columns=dict(buy='bid', sell='ask', changePrice='change24h'
                                    , high='high24h', low='low24h', volValue='quoteVolume24h'), inplace=True)
        tickers.bid = pd.to_numeric(tickers.bid)
        tickers.ask = pd.to_numeric(tickers.ask)
        tickers.changeRate = pd.to_numeric(tickers.changeRate)
        tickers.change24h = pd.to_numeric(tickers.change24h)
        tickers.high24h = pd.to_numeric(tickers.high24h)
        tickers.low24h = pd.to_numeric(tickers.low24h)
        tickers.vol = pd.to_numeric(tickers.vol)
        tickers.quoteVolume24h = pd.to_numeric(tickers.quoteVolume24h)
        tickers['last'] = pd.to_numeric(tickers['last'])
        tickers.averagePrice = pd.to_numeric(tickers.averagePrice)
        tickers.takerFeeRate = pd.to_numeric(tickers.takerFeeRate)
        tickers.makerFeeRate = pd.to_numeric(tickers.makerFeeRate)
        tickers.takerCoefficient = pd.to_numeric(tickers.takerCoefficient)
        return tickers

    def get_account_operations(self, start_time: Union[datetime, int, float, type(None)] = None,
                               end_time: Union[datetime, int, float, type(None)] = None) -> pd.DataFrame:
        self.open()
        params = {}
        self._add_date_range_params(params, start_time, end_time, 'ms')
        try:
            columns = ['amount', 'currency',
                       'status', 'createdAt', 'updatedAt', 'walletTxId']
            result = self._read_pages('/v1/deposits', params, authenticated=True)
            deposits = pd.DataFrame(result, columns=columns)
            deposits['type'] = 'deposit'
            result = self._read_pages('/v1/withdrawals', params, authenticated=True)
            withdrawals = pd.DataFrame(result, columns=columns)
            withdrawals['type'] = 'withdrawal'
            return self._to_account_operations(deposits, withdrawals)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve transactions from Kucoin') from ex

    def _to_account_operations(self, deposits: pd.DataFrame, withdrawals: pd.DataFrame):
        operations = deposits.append(withdrawals, ignore_index=True)
        operations['updatedAt'] = pd.to_datetime(operations['updatedAt'], unit='ms', utc=True)
        operations['platform'] = self.platform
        operations.rename(columns=dict(updatedAt='time', walletTxId='id',
                                       amount='size', currency='base_currency'), inplace=True)
        return operations

    def get_orders_v1(self, symbol=None, start_time: Union[datetime, int, float, type(None)] = None,
                      end_time: Union[datetime, int, float, type(None)] = None) -> pd.DataFrame:
        self.open()
        params = dict(pageSize=200)
        if symbol is not None:
            params['symbol'] = symbol
        self._add_date_range_params(params, start_time, end_time, 'ms')
        try:
            return self._read_pages('/v1/hist-orders', params, authenticated=True)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve orders v1 from Kucoin') from ex

    def _read_pages(self, path: str, params: dict, authenticated: bool, aggregated: bool = True):
        pages = self._get_pages(path, params, authenticated)
        if aggregated:
            return self._aggregate_pages(pages)
        else:
            return pages

    def _get_pages(self, path: str, params: dict, authenticated: bool = False):
        remaining_pages = True
        params['current_page'] = 0
        while remaining_pages:
            params['current_page'] += 1
            request = self._httpClient.build_request('GET', path, params=params)
            if authenticated:
                self._authenticate(request)
            response = self._httpClient.send(request)
            json = response.json()
            if not json['code'].startswith('200'):
                raise RuntimeError(f'{json["msg"]} ({json["code"]}): response code does not indicate a success')
            total_pages = json['data']['totalPage']
            remaining_pages = params['current_page'] < total_pages
            if json['data']['totalNum'] > 0:
                yield json['data']['items']

    @staticmethod
    def _aggregate_pages(iterable):
        read_orders = [json for json in iterable]
        if len(read_orders) > 0:
            return functools.reduce(lambda x, y: x + y, read_orders)
        else:
            return {}

    def _get_ws_connection_info(self, private: bool = False):
        if private:
            request = self._httpClient.build_request('POST', '/v1/bullet-private')
            self._authenticate(request)
        else:
            request = self._httpClient.build_request('POST', '/v1/bullet-public')
        try:
            return self._httpClient.send(request).json()['data']
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve websocket token from Kucoin') from ex

    def _open_websocket(self, private: bool = False):
        if self._ws is not None:
            return
        ws_cx_data = self._get_ws_connection_info(private)
        server = ws_cx_data['instanceServers'][0]
        self._ws = KucoinWebSocket(server['endpoint'], ws_cx_data['token'], server['encrypt'],
                                   server['pingInterval'], server['pingTimeout'])
        return asyncio.create_task(self._ws.open_async())

    async def register_candle_strategy_async(self, strategy, private: bool = False):
        self._open_websocket(private)
        self._ws.insert_handler(strategy)
        await self._ws.subscribe_klines_async(strategy.topics)

    async def register_ticker_strategy_async(self, strategy, private: bool = False):
        self._open_websocket(private)
        self._ws.insert_handler(strategy)
        await self._ws.subscribe_tickers_async(strategy.tickers)

    def stop_reading(self):
        if self._ws is not None:
            self._logger.debug('stopping web socket')
            self._ws.close()
            self._ws = None

    @staticmethod
    def _add_date_range_params(params: dict, start_time: Union[datetime, int, float, type(None)],
                               end_time: Union[datetime, int, float, type(None)], precision) -> dict:
        if start_time is not None:
            params['startAt'] = to_timestamp(start_time, precision) if isinstance(start_time, datetime) else int(
                round(start_time))
        if end_time is not None:
            params['endAt'] = to_timestamp(end_time, precision) if isinstance(end_time, datetime) else int(
                round(end_time))
        return params

    def _to_orders(self, orders: pd.DataFrame):
        if orders.size == 0:
            return orders
        orders.loc[:, 'time'] = pd.to_datetime(orders['createdAt'], unit='ms', utc=True)
        orders.loc[:, 'base_currency'] = orders['symbol'].apply(lambda s: s.split('-')[0])
        orders.loc[:, 'quote_currency'] = orders['symbol'].apply(lambda s: s.split('-')[1])
        if 'fee' in orders.columns:
            orders = orders.loc[:, ('id', 'base_currency', 'quote_currency', 'type', 'side', 'price',
                                    'size', 'status', 'fee', 'feeCurrency', 'time')]
            orders.loc[:, 'fee'] = pd.to_numeric(orders['fee'])
        else:
            orders = orders.loc[:, ('id', 'base_currency', 'quote_currency', 'type', 'side', 'price',
                                    'size', 'status', 'feeCurrency', 'time')]
        orders.loc[:, 'price'] = pd.to_numeric(orders['price'])
        orders.loc[:, 'size'] = pd.to_numeric(orders['size'])
        orders.astype(dict(status='string'))
        orders.loc[:, 'status'].fillna('FILLED', inplace=True)
        orders.rename(columns={'feeCurrency': 'fee_currency'})
        orders.loc[:, 'platform'] = self.platform
        return orders
