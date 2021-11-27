import json
import shutil
from datetime import datetime, timedelta
import logging
from logging.config import fileConfig

import numpy as np
import tzlocal
from dotenv import load_dotenv
from datetime import datetime


import pandas as pd
import sqlalchemy
from kucoin.client import Market

from wired_exchange import import_transactions
from wired_exchange.ftx.FTXClient import FTXClient
from wired_exchange.kucoin import KucoinClient, CandleStickResolution


def try_ftx():
    with FTXClient() as client:
        return client.get_transactions(datetime.now(tzlocal.get_localzone()) + timedelta(days=-10))


def try_kucoin():
    kucoin = Market()
    df = pd.DataFrame(kucoin.get_currencies()).set_index('currency') \
        .rename(columns={'withdrawalMinSize': 'kucoinWithdrawalMinSize',
                         'withdrawalMinFee': 'kucoinWithdrawalMinFee',
                         'isWithdrawEnabled': 'isKucoinWithdrawEnabled',
                         'isDepositEnabled': 'isKucoinDepositEnabled',
                         'isMarginEnabled': 'isKucoinMarginEnabled',
                         'isDebitEnabled': 'isKucoinDebitEnabled'
                         })
    df['kucoinWithdrawalMinSize'] = df['kucoinWithdrawalMinSize'].transform(lambda s: float(s))
    df['kucoinWithdrawalMinFee'] = df['kucoinWithdrawalMinFee'].transform(lambda s: float(s))
    db = sqlalchemy.create_engine('sqlite:///wired_exchange.db')
    df.to_sql('CURRENCIES', db, if_exists='replace', index=True
              , dtype={'currency': sqlalchemy.types.NVARCHAR(5), 'name': sqlalchemy.types.NVARCHAR(20),
                       'fullName': sqlalchemy.types.NVARCHAR(80), 'precision': sqlalchemy.types.INTEGER,
                       'confirms': sqlalchemy.types.INTEGER, 'contractAddress': sqlalchemy.types.NVARCHAR(255),
                       'kucoinWithdrawalMinSize': sqlalchemy.types.FLOAT,
                       'kucoinWithdrawalMinFee': sqlalchemy.types.FLOAT,
                       'isKucoinWithdrawEnabled': sqlalchemy.types.BOOLEAN,
                       'isKucoinDepositEnabled': sqlalchemy.types.BOOLEAN,
                       'isKucoinMarginEnabled': sqlalchemy.types.BOOLEAN,
                       'isKucoinDebitEnabled': sqlalchemy.types.BOOLEAN})

    # klines = kucoin.get_kline('BTCUSDT', '1d')
    # print(klines)


def get_kucoin_symbols():
    kucoin = Market()
    return kucoin.get_symbol_list()


def get_kucoin_markets():
    kucoin = Market()
    return kucoin.get_market_list()


def get_kucoin_kline():
    kucoin = Market()
    return kucoin.get_kline('BTC-USDT', '1min',
                            startAt=int(round(datetime.fromisoformat('2021-11-11T21:53:00+01:00').timestamp())),
                            endAt=int(round(datetime.fromisoformat('2021-11-11T21:55:00+01:00').timestamp())))


def to_json(data, filepath: str):
    with open(filepath, 'w') as outfile:
        json.dump(data, outfile)


def from_json(filepath: str):
    with open(filepath, 'w') as outfile:
        return json.load(outfile)


kucoin_sandbox = False

if kucoin_sandbox:
    load_dotenv('.env-sandbox')
    kucoin_host_url = 'https://openapi-sandbox.kucoin.com'
else:
    load_dotenv()
    kucoin_host_url = None

logging.config.fileConfig('logging.conf')

logger = logging.getLogger('main')
logger.info('--------------------- starting Wired Exchange ---------------------')

#notebook_folder = 'D:\\dev\\study\\Study.AlgorithmicTrading\\algorithmic-trading-python\\data'

#     to_json(kucoin.get_transactions(start_time=datetime.fromisoformat('2021-11-10T21:53:00+01:00')), 'kucoin_transactions-sandbox.json')
#     to_json(kucoin.get_orders(start_time=datetime.fromisoformat('2021-11-10T21:53:00+01:00')), 'kucoin_orders-sandbox.json')

# tr = pd.read_json('data/ftx_transactions.json')
# tr['time'] = pd.to_datetime(tr['time'])
# tr = tr.astype({'market': 'string', 'baseCurrency': 'string', 'quoteCurrency': 'string', 'type': 'string',
#                 'side': 'string', 'feeCurrency': 'string', 'liquidity': 'string'})

# shutil.copyfile('\\'.join([notebook_folder, 'ftx_transactions.json']), 'data/ftx_transactions.json')
# shutil.copyfile('\\'.join([notebook_folder, 'kucoin_transactions.json']), 'data/kucoin_transactions.json')
#     ftx.enrich_usd_prices(tr)
# tr.to_json('data/kucoin_transactions.json', orient='records', date_format='iso')
# prices = pd.read_json('data/ftx_usd_prices_1h.json')
# pd.read_json('Data/kucoin_transactions.json')

# tr = read_transactions('data/ftx_transactions.json')
# tr.append(read_transactions('data/kucoin_transactions.json'))
# print(tr)

import_transactions('EBL')
with KucoinClient() as kucoin:
    print(kucoin.get_prices('BTC', 'USDT', CandleStickResolution._1min, start_time=datetime.fromisoformat('2021-11-27T21:53:00+01:00')))

logger.info('--------------------- Wired Exchange stopped ---------------------')
