import logging

import pandas as pd

from wired_exchange import FTXClient, KucoinClient, WiredStorage
from wired_exchange.core import to_transactions


class Portfolio:
    def __init__(self, profile: str):
        self.profile = profile
        self._db = WiredStorage(self.profile)
        self._logger = logging.getLogger(type(self).__name__)

    def import_transactions(self, start_time) -> pd.DataFrame:
        with FTXClient() as ftx:
            tr = ftx.get_transactions(start_time=start_time)
            with KucoinClient() as kucoin:
                kucoin_tr, _ = ftx.enrich_usd_prices(
                    kucoin.get_transactions(start_time=start_time))
        tr = tr.append(kucoin_tr)
        self._db.save_transactions(tr)
        return tr

    def import_account_operations(self, start_time) -> pd.DataFrame:
        with KucoinClient() as kucoin:
            ops = kucoin.get_account_operations(start_time)
            if ops.size == 0:
                return ops
            tr = pd.DataFrame(ops[ops['status'] == 'SUCCESS'],
                              columns=['amount', 'currency', 'updatedAt', 'type', 'platform', 'walletTxId'])
            tr.rename(columns={'currency': 'base_currency', 'amount': 'size'
                , 'updatedAt': 'time', 'walletTxId': 'id'}, inplace=True)
            tr['id'] = tr.apply(lambda row: f'{row["platform"]}_{row["id"]}', axis=1)
            tr.set_index('id', inplace=True)
            self._db.save_transactions(tr)
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
                tr['average_price_usd'] = current_usdt_price * tr['average_price']
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
        tr['amount_usd'] = tr['size'] * tr['price'] * tr['price_usd']
        by_currencies = tr[buy_trs & orders & (~quote_currencies)].groupby('base_currency')
        return by_currencies['amount_usd'].sum() / by_currencies['size'].sum()

    def get_summary(self):
        p = self.get_positions()
        abp = self.get_average_buy_prices()
        if abp.size == 0:
            return p
        summary = pd.concat([p, abp], axis=1)
        summary.rename(columns={0: 'average_buy_price_usd'}, inplace=True)
        return summary
