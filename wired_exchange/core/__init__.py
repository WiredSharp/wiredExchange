import os

import pandas as pd
import pytomlpp

resource_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "resources")
config_path = os.path.join(resource_path, 'wired_exchange.toml')

VERSION = '1.0.0'

with open(config_path, 'r') as cfg:
    _config = pytomlpp.load(cfg)


def config():
    return _config


def merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def to_timestamp(dt, resolution) -> int:
    return to_timestamp_in_seconds(dt) if resolution == 's' else to_timestamp_in_milliseconds(dt)

def to_timestamp_in_seconds(dt) -> int:
    return int(round(dt.timestamp()))


def to_timestamp_in_milliseconds(dt) -> int:
    return int(round(dt.timestamp() * 1000))


def read_transactions(path_or_buf, orient='index') -> pd.DataFrame:
    tr = pd.read_json(path_or_buf, orient=orient)
    return to_transactions(tr)


def to_transactions(tr) -> pd.DataFrame:
    tr = tr.astype(
        dict(base_currency='string', quote_currency='string', side='string',
             fee_currency='string', price='float', size='float', fee='float', platform='string'))
    if 'id' in tr.columns:
        tr.set_index('id', inplace=True)
    return tr


def read_klines(path_or_buf, base: str = None, quote: str = None) -> pd.DataFrame:
    return to_klines(pd.read_json(path_or_buf), base, quote)


def to_klines(pr, base: str = None, quote: str = None) -> pd.DataFrame:
    if (base is not None) and ('base_currency' not in pr.columns):
        pr['base_currency'] = base
    if (quote is not None) and ('quote_currency' not in pr.columns):
        pr['quote_currency'] = quote
    if 'base_currency' in pr.columns:
        pr.astype(dict(base_currency='string'))
    if 'quote_currency' in pr.columns:
        pr.astype(dict(quote_currency='string'))
    pr['time'] = pd.to_datetime(pr['time'], unit='ms', utc=True)
    pr.set_index('time', inplace=True)
    pr.astype(dict(open='float', high='float', low='float', close='float', volume='float'))
    return pr
