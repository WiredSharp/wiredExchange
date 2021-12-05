import logging

import pandas as pd
from datetime import datetime
from wired_exchange import FTXClient, KucoinClient, WiredStorage
from wired_exchange.core import to_transactions


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
            with KucoinClient() as kucoin:
                try:
                    kucoin_tr, _ = ftx.enrich_usd_prices(
                        kucoin.get_transactions(start_time=start_time))
                    tr = tr.append(kucoin_tr)
                except:
                    self._logger.error('cannot retrieve operations from Kucoin', exc_info=True)
        self._db.save_transactions(tr)
        return tr

    def import_account_operations(self, start_time: datetime = None) -> pd.DataFrame:
        if start_time is None:
            start_time = self._get_last_transaction_time()
        ops = None
        with KucoinClient() as kucoin:
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
        with KucoinClient() as kucoin:
            try:
                tr = kucoin.get_balances()
            except:
                self._logger.error('cannot retrieve balances from Kucoin', exc_info=True)
                tr = pd.DataFrame()
        with FTXClient() as ftx:
            try:
                tr = tr.append(ftx.get_balances())
            except:
                self._logger.error('cannot retrieve balances from FTX', exc_info=True)
            try:
                current_usdt_price = ftx.resolve_current_price('USDT', 'USD')
                tr['price_usd'] = current_usdt_price * tr['price']
            except:
                self._logger.error('cannot retrieve USDT current price from FTX', exc_info=True)
        return tr

    def get_average_buy_prices(self):
        tr = self.get_transaction()
        if tr.size == 0:
            return tr
        buy_trs = tr['side'] == 'buy'
        orders = tr['type'].isin(['order', 'limit', 'market'])
        quote_currencies = tr['base_currency'].isin(['USD', 'USDT'])
        tr['amount'] = tr['size'] * tr['price']
        tr['amount_usd'] = tr['amount'] * tr['price_usd']
        by_currencies = tr[buy_trs & orders & (~quote_currencies)].groupby('base_currency')
        size_sum = by_currencies['size'].sum()
        average_prices = by_currencies['amount'].sum() / size_sum
        average_usd_prices = by_currencies['amount_usd'].sum() / size_sum
        average_prices = pd.concat([average_prices, average_usd_prices], axis=1)
        average_prices.rename(columns={0: 'average_buy_price', 1: 'average_buy_price_usd'}, inplace=True)
        return average_prices

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

    def _get_last_transaction_time(self) -> datetime:
        return self._db.read_transactions()['time'].max()
