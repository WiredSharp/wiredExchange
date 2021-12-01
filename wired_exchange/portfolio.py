import pandas as pd

from wired_exchange import FTXClient, KucoinClient, WiredStorage, to_transactions


class Portfolio:
    def __init__(self, profile: str):
        self.profile = profile
        self._db = WiredStorage(self.profile)

    def import_transactions(self, start_time) -> pd.DataFrame:
        with FTXClient() as ftx:
            tr = ftx.get_transactions()
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
            tr.rename(columns={'currency': 'base_currency', 'amount': 'size', 'updatedAt': 'time', 'walletTxId': 'id'}, inplace=True)
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
        with FTXClient() as ftx:
            tr = ftx.get_balances()
        with KucoinClient() as kucoin:
            tr = tr.append(kucoin.get_balances())
        return tr

    def get_average_buy_prices(self):
        tr = self.get_transaction()
        if tr.size == 0:
            return tr
        buy_trs = tr['side'] == 'buy'
        orders = tr['type'].isin(['order', 'limit'])
        tr['amount_usd'] = tr['size'] * tr['price'] * tr['price_usd']
        byCurrency = tr[buy_trs & orders].groupby('base_currency')
        return byCurrency['amount_usd'].sum() / byCurrency['size'].sum()

    def get_summary(self):
        p = self.get_positions()
        abp = self.get_average_buy_prices()
        if abp.size == 0:
            return p
        summary = pd.concat([p.groupby('currency').sum(), abp], axis=1)
        summary.rename(columns={'0': 'average_buy_price'}, inplace=True)
        return summary
