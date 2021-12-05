import logging

import pytomlpp
import os.path

from datetime import datetime

from wired_exchange.ftx import FTXClient
from wired_exchange.kucoin import KucoinClient
from wired_exchange.storage import WiredStorage
from wired_exchange.core import config, merge


if os.path.exists('wired_exchange.toml'):
    with open('wired_exchange.toml', 'r') as cfg:
        merge(config(), pytomlpp.load(cfg))


def import_transactions(profile: str):
    with FTXClient() as ftx:
        tr = ftx.get_transactions()
        with KucoinClient() as kucoin:
            kucoin_tr, _ = ftx.enrich_usd_prices(
                kucoin.get_transactions(start_time=datetime.fromisoformat('2021-11-10T21:53:00+01:00')))
    tr = tr.append(kucoin_tr)
    with WiredStorage(profile) as db:
        db.save_transactions(tr)
    return tr