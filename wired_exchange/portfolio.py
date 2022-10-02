import logging

import pandas as pd
from collections import namedtuple
from datetime import datetime, timezone
from typing import Union
from wired_exchange import WiredStorage, KucoinSpotClient
from wired_exchange.bitpandapro import BitPandaProClient
from wired_exchange.ftx import FTXClient
from wired_exchange.core import to_transactions
from wired_exchange.kucoin import KucoinFuturesClient


class Portfolio:
    def __init__(self, profile: str):
        self.profile = profile
        self._db = WiredStorage(self.profile)
        self._logger = logging.getLogger(type(self).__name__)

    def import_transactions(self, start_time: datetime = None) -> pd.DataFrame:
        if start_time is None:
            start_time = self._get_last_transaction_time()
        with FTXClient() as ftx:
            tr = ftx.get_transactions(start_time=start_time)
            with KucoinSpotClient() as kucoin:
                try:
                    kucoin_tr, _ = ftx.enrich_usd_prices(kucoin.get_transactions(start_time=start_time))
                    tr = tr.append(kucoin_tr)
                except:
                    self._logger.error('cannot retrieve transactions from Kucoin', exc_info=True)
            with BitPandaProClient() as bp:
                try:
                    bp_tr, _ = ftx.enrich_usd_prices(bp.get_transactions(start_time=start_time,
                                                                         end_time=datetime.now(timezone.utc)))
                    tr = tr.append(bp_tr)
                except:
                    self._logger.error('cannot retrieve transactions from BitPanda Pro', exc_info=True)
        self._db.save_transactions(tr)
        return tr

    def import_account_operations(self, start_time: datetime = None) -> pd.DataFrame:
        if start_time is None:
            start_time = self._get_last_transaction_time()
        ops = None
        with KucoinSpotClient() as kucoin:
            try:
                kucoin_ops = kucoin.get_account_operations(start_time)
                if kucoin_ops.size > 0:
                    ops = pd.DataFrame(kucoin_ops[kucoin_ops['status'] == 'SUCCESS'],
                                       columns=['size', 'base_currency', 'id', 'type', 'platform', 'time'])
            except:
                self._logger.error('cannot retrieve operations from Kucoin', exc_info=True)
        with FTXClient() as ftx:
            try:
                ftx_ops = ftx.get_account_operations(start_time)
                if ftx_ops.size > 0:
                    ftx_ops = pd.DataFrame(ftx_ops[ftx_ops['status'].isin(['confirmed', 'complete'])],
                                           columns=['size', 'base_currency', 'id', 'type', 'platform', 'time'])
                    if ops is not None:
                        ops = ops.append(ftx_ops)
                    else:
                        ops = ftx_ops
            except:
                self._logger.error('cannot retrieve operations from FTX', exc_info=True)
        if ops is not None:
            ops['id'] = ops.apply(lambda row: f'{row["platform"]}_{row["id"]}', axis=1)
            ops.set_index('id', inplace=True)
            self._db.save_transactions(ops)
        return ops

    def append_transactions(self, transactions: pd.DataFrame):
        self._db.save_transactions(transactions)
        return self

    def get_transaction(self):
        tr = self._db.read_transactions()
        if tr.size == 0:
            return tr
        tr['time'] = pd.to_datetime(tr['time'])
        return to_transactions(tr)

    def get_positions(self):
        positions = pd.DataFrame()
        with KucoinSpotClient() as kucoin:
            try:
                positions = positions.append(kucoin.get_balances())
            except:
                self._logger.error('cannot retrieve balances from Kucoin', exc_info=True)
        with BitPandaProClient() as bp:
            try:
                positions = positions.append(bp.get_balances())
            except:
                self._logger.error('cannot retrieve balances from BitPanda Pro', exc_info=True)
        with FTXClient() as ftx:
            try:
                positions = positions.append(ftx.get_balances())
            except:
                self._logger.error('cannot retrieve balances from FTX', exc_info=True)
            try:
                usdt_usd_rate = ftx.get_live_rate('USDT', 'USD')
                usd_usdt_rate = 1 / usdt_usd_rate
                positions['price'] = usd_usdt_rate * positions[pd.isna(positions['price']), ['price_usd']]
                positions['price'] = positions[pd.isna(positions['price']), 'currency']\
                    .apply(lambda c: ftx.get_live_rate(c, 'USDT'))
                positions['price_usd'] = usdt_usd_rate * positions[pd.isna(positions['price_usd']), ['price']]
            except:
                self._logger.error('cannot enrich prices', exc_info=True)
        return positions

    def get_average_buy_prices(self):
        tr = self.get_transaction()
        if tr.size == 0:
            return tr
        tr.dropna(subset=['side'], inplace=True)
        tr.sort_values(by=['time'], inplace=True)
        quote_currencies = tr['base_currency'].isin(['USD', 'USDT', 'CHF'])
        positions = {}
        Position = namedtuple('Position', ['size', 'price', 'price_usd'])
        for transaction in tr[~quote_currencies].itertuples():
            position = positions.get(transaction.base_currency)
            if position is None:
                position = Position(transaction.size, transaction.price, transaction.price_usd * transaction.price)
            else:
                if transaction.side.lower() == 'sell':
                    position = Position(position.size - transaction.size, position.price, position.price_usd)
                else:  # buy
                    position = Position(position.size + transaction.size,
                                        (position.size * position.price + transaction.size * transaction.price)
                                        / (position.size + transaction.size),
                                        transaction.price_usd *
                                        (position.size * position.price + transaction.size * transaction.price)
                                        / (position.size + transaction.size))
            positions[transaction.base_currency] = position

        # orders = tr['type'].isin(['order', 'limit', 'market'])
        # quote_currencies = tr['base_currency'].isin(['USD', 'USDT', 'CHF'])
        # tr['amount'] = np.where(tr['side'] == 'buy', tr['size']*tr['price'], np.nan)
        # tr['amount_usd'] = tr['amount'] * tr['price_usd']
        # by_currencies = tr[orders & (~quote_currencies)].groupby('base_currency')
        # size_sum = by_currencies['size'].sum()
        # average_prices = by_currencies['amount'].sum() / size_sum
        # average_usd_prices = by_currencies['amount_usd'].sum() / size_sum
        # average_prices = pd.concat([average_prices, average_usd_prices], axis=1)
        # average_prices.rename(columns={0: 'average_buy_price', 1: 'average_buy_price_usd'}, inplace=True)
        return pd.DataFrame(positions.values(), index=positions.keys(),
                            columns=['size', 'average_buy_price', 'average_buy_price_usd'])

    def get_summary(self):
        p = self.get_positions()
        abp = self.get_average_buy_prices()
        if abp.size == 0:
            return p
        summary = pd.concat([p, abp], axis=1)
        summary['PnL_pc'] = (summary['price_usd'] - summary['average_buy_price_usd']) / summary[
            'average_buy_price_usd'] * 100
        summary['PnL_tt'] = (summary['price_usd'] - summary['average_buy_price_usd']) * summary['total']
        return summary

    def get_orders(self, start_time: Union[datetime, int, float, type(None)] = None):
        if start_time is None:
            start_time = self._get_first_transaction_time()
        ops = None
        with KucoinSpotClient() as kucoin:
            try:
                kucoin_ops = kucoin.get_orders(start_time=start_time, status='done')
                if kucoin_ops.size > 0:
                    ops = pd.DataFrame(kucoin_ops)
                kucoin_ops = kucoin.get_orders(start_time=start_time, status='active')
                if kucoin_ops.size > 0:
                    if ops is not None:
                        ops = ops.append(kucoin_ops)
                    else:
                        ops = kucoin_ops
            except:
                self._logger.error('cannot retrieve orders from Kucoin', exc_info=True)
        with FTXClient() as ftx:
            try:
                ftx_ops = ftx.get_orders(start_time)
                if ftx_ops.size > 0:
                    if ops is not None:
                        ops = ops.append(ftx_ops)
                    else:
                        ops = ftx_ops
            except:
                self._logger.error('cannot retrieve orders from FTX', exc_info=True)
        with BitPandaProClient() as bp:
            try:
                bp_ops = bp.get_orders(start_time, end_time=datetime.now(timezone.utc), include_filled=False)
                if bp_ops.size > 0:
                    if ops is not None:
                        ops = ops.append(bp_ops)
                    else:
                        ops = bp_ops
            except:
                self._logger.error('cannot retrieve orders from BitPanda Pro', exc_info=True)
        if ops is not None:
            ops.drop_duplicates(subset=['id'], inplace=True)
            ops['id'] = ops.apply(lambda row: f'{row["platform"]}_{row["id"]}', axis=1)
            # ops.set_index('id', inplace=True)
            ops.sort_values(by=['time'], ascending=False, inplace=True)
        return ops

    def get_futures(self):
        with KucoinFuturesClient() as futures:
            return futures.get_positions()

    def _get_last_transaction_time(self) -> datetime:
        return self._db.read_transactions()['time'].max().to_pydatetime()

    def _get_first_transaction_time(self) -> datetime:
        return self._db.read_transactions()['time'].min().to_pydatetime()
