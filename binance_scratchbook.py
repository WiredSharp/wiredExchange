import os
import logging
from logging.config import fileConfig

from dotenv import load_dotenv
from binance.client import Client

load_dotenv('.env-polene')

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('main')

logger.info('--------------------- starting Wired Exchange ---------------------')
try:
    binance = Client(os.getenv('binance_api_key'), os.getenv('binance_api_secret'))
    account = binance.get_account()
    print(account)
except:
    logger.fatal('execution failed', exc_info=True)
logger.info('--------------------- Wired Exchange stopped ---------------------')
