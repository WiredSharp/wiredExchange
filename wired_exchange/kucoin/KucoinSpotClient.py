import asyncio
import functools
import time
import uuid
from datetime import datetime, timedelta
from typing import Literal, Union

import httpx
import pandas as pd
import tzlocal
from pandas import DataFrame

from wired_exchange.core import to_timestamp, to_transactions, to_klines, from_timestamp
from wired_exchange.core.ExchangeClient import ExchangeClient
from wired_exchange.kucoin import CandleStickResolution
from wired_exchange.kucoin.KucoinAuthenticator import KucoinAuthenticator
from wired_exchange.kucoin.WebSocket import KucoinWebSocket

QUERY_MAX_DAYS_RANGE = 7


class KucoinSpotClient(ExchangeClient):

    def __init__(self, api_key: str = None, api_passphrase: str = None,
                 api_secret: str = None, host_url: str = None):
        super().__init__('kucoin', api_key, api_secret, host_url, always_authenticate=False)
        self._api_passphrase = api_passphrase if api_passphrase is not None else self._get_exchange_env_value(
            'api_passphrase')
        self._ws = None
        self._authenticator = KucoinAuthenticator(self._api_key, self._api_passphrase, self._api_secret)

    def _authenticate(self, request):
        self._authenticator.authenticate(request)

    def get_transactions(self, start_time: datetime, end_time: datetime = None,
                         trade_type: Literal['spot', 'margin'] = 'spot') -> pd.DataFrame:
        self.open()
        if end_time is None:
            end_time = datetime.now(tzlocal.get_localzone())
        else:
            if not isinstance(end_time, datetime):
                end_time = from_timestamp(end_time)
        if not isinstance(start_time, datetime):
            start_time = from_timestamp(start_time)
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        transactions = None
        try:
            for p in self._get_date_ranges(params, start_time, end_time):
                current_transactions = self._read_pages('/v1/fills', p, authenticated=True)
                if len(current_transactions) > 0:
                    transactions = pd.DataFrame(current_transactions) if transactions is None else transactions.append(
                        pd.DataFrame(current_transactions), ignore_index=True)
            if transactions is not None:
                return self._to_transactions(transactions)
            else:
                return pd.DataFrame()
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
        orders = None
        try:
            for p in self._get_date_ranges(params, start_time, end_time):
                for path in ['/v1/orders', '/v1/stop-order']:
                    current_orders = self._read_pages(path, params=p, authenticated=True)
                    if len(current_orders) > 0:
                        orders = pd.DataFrame(current_orders) if orders is None else orders.append(
                            pd.DataFrame(current_orders), ignore_index=True)
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
        self._set_date_range_params(params, start_time, end_time, 's')
        try:
            request = self._httpClient.build_request('GET', '/v1/market/candles', params=params)
            response = self._httpClient.send(request).json()
            if not response['code'].startswith('200'):
                raise RuntimeError(f'{response["code"]}: response code does not indicate a success')
            return self._to_klines(response['data'], base_currency, quote_currency)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve transactions from Kucoin') from ex

    def _to_transactions(self, fills: Union[DataFrame, dict]) -> pd.DataFrame:
        tr = pd.DataFrame(fills) if isinstance(fills, dict) else fills
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
        tr['id'] = tr['trade_id'].apply(lambda t_id: f'{self.platform}_{t_id}')
        return to_transactions(tr)

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
        balances.drop(columns=['averagePrice'], inplace=True)
        balances.rename(columns=dict(balance='total', last='price'), inplace=True)
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
        if end_time is None:
            end_time = datetime.now(tzlocal.get_localzone())
        else:
            if not isinstance(end_time, datetime):
                end_time = from_timestamp(end_time)
        if not isinstance(start_time, datetime):
            start_time = from_timestamp(start_time)
        deposits = None
        withdrawals = None
        columns = ['amount', 'currency', 'status', 'createdAt', 'updatedAt', 'walletTxId']
        try:
            for p in self._get_date_ranges(params, start_time, end_time):
                current_deposits = self._read_pages('/v1/deposits', p, authenticated=True)
                if len(current_deposits) > 0:
                    current_deposits = pd.DataFrame(current_deposits, columns=columns)
                    current_deposits['type'] = 'deposit'
                    deposits = current_deposits if deposits is None else deposits.append(current_deposits,
                                                                                         ignore_index=True)
                current_withdrawals = self._read_pages('/v1/withdrawals', p, authenticated=True)
                if len(current_withdrawals) > 0:
                    current_withdrawals = pd.DataFrame(current_withdrawals, columns=columns)
                    current_withdrawals['type'] = 'withdrawal'
                    withdrawals = current_withdrawals if withdrawals is None else withdrawals.append(
                        current_withdrawals, ignore_index=True)
            return self._to_account_operations(deposits, withdrawals)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve transactions from Kucoin') from ex

    def _to_account_operations(self, deposits: pd.DataFrame, withdrawals: pd.DataFrame):
        if deposits is None and withdrawals is None:
            return pd.DataFrame()
        if deposits is None:
            operations = withdrawals
        else:
            if withdrawals is None:
                operations = deposits
            else:
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
        self._set_date_range_params(params, start_time, end_time, 'ms')
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
            retry = True
            response = None
            while retry:
                if authenticated:
                    self._authenticate(request)
                try:
                    response = self._httpClient.send(request)
                    retry = False
                except httpx.HTTPStatusError as ex:
                    if 429 == ex.response.status_code:
                        retry = True
                        self._logger.warning('request threshold reach, waiting 10s...')
                        time.sleep(11)
                    else:
                        raise ex
            json = response.json()
            if not json['code'].startswith('200'):
                raise RuntimeError(f'{json["msg"]} ({json["code"]}): response code does not indicate a success')
            total_pages = json['data']['totalPage']
            remaining_pages = params['current_page'] < total_pages
            if json['data']['totalNum'] > 0:
                yield json['data']['items']

    def _get_date_ranges(self, params: dict, start_time: datetime, end_time: datetime = None) -> dict:
        last_query = False
        if end_time is None:
            end_time = datetime.now(tzlocal.get_localzone())
        while not last_query:
            date_range = end_time - start_time
            if date_range > timedelta(0):
                if date_range > timedelta(days=QUERY_MAX_DAYS_RANGE):
                    last_query = False
                    self._set_date_range_params(params, start_time, start_time
                                                + timedelta(days=QUERY_MAX_DAYS_RANGE), 'ms')
                    start_time = start_time + timedelta(days=QUERY_MAX_DAYS_RANGE)
                else:
                    last_query = True
                    self._set_date_range_params(params, start_time, end_time, 'ms')
                yield params

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
    def _set_date_range_params(params: dict, start_time: Union[datetime, int, float, type(None)],
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
        if 'status' not in orders.columns:
            orders['status'] = pd.NA
        if 'isActive' in orders.columns:
            orders.loc[:, 'isActive'].fillna(True, inplace=True)
            orders.loc[(orders['isActive'] == False) & orders['status'].isna(), 'status'] = 'FILLED'
            orders.loc[(orders['isActive'] == True) & orders['status'].isna(), 'status'] = 'NEW'
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
        orders.rename(columns={'feeCurrency': 'fee_currency'})
        orders.loc[:, 'platform'] = self.platform
        return orders

    def place_order(self, symbol: str, side: Literal['buy', 'sell'], limit: float, stop: float = None,
                    size: float = None, amount: float = None, take_profit_pct: float = None,
                    stop_loss_pct: float = None, remark: str = None):
        self.open()
        str(uuid.uuid4())
        data = dict(clientOid=str(uuid.uuid4()), symbol=symbol, side=side, price=limit)
        if stop is not None:
            path = '/v1/stop-order'
            data['stopPrice'] = stop
        else:
            path = '/v1/orders'
        if size is not None:
            data['size'] = size
        else:
            if amount is not None:
                data['size'] = amount / limit
        if remark is not None:
            data['remark'] = remark
        try:
            request = self._httpClient.build_request('POST', path, json=data)
            self._authenticate(request)
            response = self._httpClient.send(request).json()
            # if not response['code'].startswith('200'):
            #     raise RuntimeError(f'{response["code"]}: response code does not indicate a success')
            return response
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot place order') from ex
