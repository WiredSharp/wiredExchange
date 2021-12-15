import os
import logging
from logging.config import fileConfig

import pandas as pd
from dotenv import load_dotenv

from wired_exchange.binance import BinanceClient

load_dotenv('.env-polene')

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('main')

logger.info('--------------------- starting Wired Exchange ---------------------')
try:
    binance = BinanceClient(os.getenv('binance_api_key'), os.getenv('binance_api_secret'))
    # balances = binance.get_balances()
    # print(balances)
    transactions = binance.get_transactions()[['price', 'size', 'amount', 'status', 'side', 'time']]
    print(transactions)
except:
    logger.fatal('execution failed', exc_info=True)
logger.info('--------------------- Wired Exchange stopped ---------------------')
