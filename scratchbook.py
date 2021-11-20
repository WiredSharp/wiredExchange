from datetime import datetime, timedelta
import logging
import tzlocal
from dotenv import load_dotenv

import pandas as pd
import sqlalchemy
from kucoin.client import Market

from wired_exchange.ftx.FTXClient import FTXClient

load_dotenv()

logging.basicConfig(filename='wired_exchange.log', encoding='utf-8', level=logging.DEBUG
                    , format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')

logger = logging.getLogger('main')

def try_ftx():
    with FTXClient() as client:
        fills = client.get_transactions(datetime.now(tzlocal.get_localzone()) + timedelta(days=-10))
        print(fills)

def try_kucoin():
    kucoin = Market()
    df = pd.DataFrame(kucoin.get_currencies()).set_index('currency')\
        .rename(columns={'withdrawalMinSize': 'kucoinWithdrawalMinSize',
                         'withdrawalMinFee': 'kucoinWithdrawalMinFee',
                         'isWithdrawEnabled': 'isKucoinWithdrawEnabled',
                         'isDepositEnabled': 'isKucoinDepositEnabled',
                         'isMarginEnabled': 'isKucoinMarginEnabled',
                         'isDebitEnabled': 'isKucoinDebitEnabled'
                         })
    df['kucoinWithdrawalMinSize'] = df['kucoinWithdrawalMinSize'].transform(lambda s: float(s))
    df['kucoinWithdrawalMinFee'] = df['kucoinWithdrawalMinFee'].transform(lambda s: float(s))
    db = sqlalchemy.create_engine('sqlite:///wiredexchange.db')
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

    #klines = kucoin.get_kline('BTCUSDT', '1d')
    #print(klines)


logger.info('--------------------- starting Wired Exchange ---------------------')
try_kucoin()
logger.info('--------------------- Wired Exchange stopped ---------------------')
