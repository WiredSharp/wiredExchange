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

    def append_transactions(self, transactions: pd.DataFrame):
        self._db.save_transactions(transactions)
        return self

    def get_transaction(self):
        tr = self._db.read_transactions()
        tr['time'] = pd.to_datetime(tr['time'])
        return to_transactions(tr)

    def get_positions(self):
        tr = self.get_transaction()
