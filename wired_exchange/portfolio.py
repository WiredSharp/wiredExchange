import pandas as pd

from wired_exchange import FTXClient, KucoinClient, WiredStorage


class Portfolio:
    def __init__(self, profile: str):
        self.profile = profile

    def import_transactions(self, start_time) -> pd.DataFrame:
        with FTXClient() as ftx:
            tr = ftx.get_transactions()
            with KucoinClient() as kucoin:
                kucoin_tr, _ = ftx.enrich_usd_prices(
                    kucoin.get_transactions(start_time=start_time))
        tr = tr.append(kucoin_tr)
        with WiredStorage(self.profile) as db:
            db.save_transactions(tr)
        return tr

    def append_transactions(self, transactions: pd.DataFrame):
        raise NotImplementedError('TODO')