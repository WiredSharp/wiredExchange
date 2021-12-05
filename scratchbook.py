import asyncio
import json
import shutil
from datetime import datetime, timedelta
import logging
from logging.config import fileConfig

import tzlocal
from dotenv import load_dotenv
from datetime import datetime


import pandas as pd
import sqlalchemy
from kucoin.client import Market

import xlsxwriter

from wired_exchange import import_transactions, WiredStorage
from wired_exchange.core import read_transactions
from wired_exchange.ftx.FTXClient import FTXClient
from wired_exchange.kucoin import KucoinClient, CandleStickResolution
from wired_exchange.portfolio import Portfolio

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

# shutil.copyfile('\\'.join([notebook_folder, 'ftx_transactions.json']), 'data/ftx_transactions.json')
# shutil.copyfile('\\'.join([notebook_folder, 'kucoin_transactions.json']), 'data/kucoin_transactions.json')
#     ftx.enrich_usd_prices(tr)
# tr.to_json('data/kucoin_transactions.json', orient='records', date_format='iso')
# prices = pd.read_json('data/ftx_usd_prices_1h.json')
# pd.read_json('Data/kucoin_transactions.json')

# tr = read_transactions('data/ftx_transactions.json')
# tr.append(read_transactions('data/kucoin_transactions.json'))
# print(tr)

# import_transactions('EBL')
# with KucoinClient() as kucoin:

# db = WiredStorage('EBL')
# with FTXClient() as ftx:
#     ftx.get_account_operations()
     # ftx.get_balances().to_json('data/ftx_balances.json', orient='index', date_format='iso')
#
# with KucoinClient() as kucoin:
#     print(kucoin.get_account_operations())
#     print(kucoin.get_prices('BTC', 'USDT', CandleStickResolution._1min,
#                             start_time=datetime.fromisoformat('2021-11-27T21:53:00+01:00'),
#                             end_time=datetime.fromisoformat('2021-11-28T00:53:00+01:00')))
#     print(kucoin.get_balances())
#     tr = ftx.get_transactions()
# with KucoinClient() as kucoin:
    # operations = kucoin.get_account_operations(start_time=datetime.fromisoformat('2021-11-10T21:53:00+01:00'))
    # print(operations)
    # orders = kucoin.get_orders_v1(start_time=datetime.fromisoformat('2021-11-10T21:53:00+01:00'))
    # print(orders)
    # asyncio.run(kucoin.read_ws_async())
#         print(ktr)
#         kucoin_tr, _ = ftx.enrich_usd_prices(ktr)
# tr = tr.append(kucoin_tr)
#
# tr.to_json('data/ebr_transactions.json', orient='index', date_format='iso')

# tr = read_transactions('data/ebr_transactions.json')

# tr = db.read_transactions()
# tr.to_json('data/ebr_transactions.json', orient='index', date_format='iso')
# db.save_transactions(tr)

wallet = Portfolio('EBL')
#
wallet.import_account_operations(datetime.fromisoformat('2021-11-10T18:53:00+01:00'))
# wallet.import_account_operations()
# wallet.import_transactions(datetime.fromisoformat('2021-12-04T18:53:00+01:00'))
# wallet.get_summary().to_csv('data/positions.csv')
# p = wallet.get_positions()
# print(p)
# print(wallet.get_average_buy_prices())
# print(p['platform'])
# tr = wallet.get_transaction()
# tr['time']=tr['time'].astype('string')
# wallet.get_transaction().to_json('data/ebr_wallet_transactions.json', orient='index', date_format='iso')
# tr.to_excel('data/ebr_wallet_transactions.xlsx', engine='xlsxwriter')
# print(wallet.get_transaction())

logger.info('--------------------- Wired Exchange Ended ---------------------')
