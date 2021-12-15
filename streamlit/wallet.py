import logging
import os
from logging.config import fileConfig
from dotenv import load_dotenv
from wired_exchange.portfolio import Portfolio
import streamlit as st

from datetime import date
import glob

logging.config.fileConfig('logging.conf')


def get_profiles():
    return [os.path.basename(p.split('_')[0]) for p in glob.glob('./*.sqlite')]


def foreground_by_sign(v):
    return 'color: red' if v < 0 else 'color: green' if v > 0 else ""


def import_all(wallet: Portfolio):
    wallet.import_account_operations()
    wallet.import_transactions()


logger = logging.getLogger('main')
logger.info('--------------------- starting Wired Exchange ---------------------')
profile = st.selectbox("select portfolio", get_profiles())

load_dotenv(f'.env-{profile}')
wallet = Portfolio(profile)
if st.button('Update Wallet'):
    import_all(wallet)

st.title(f'{profile} Wallet')
summary = wallet.get_summary()
summary = summary[(summary['total'] > .0001) & (summary.index != 'USDT') & (summary.index != 'USD')]
st.text("Positions:")
st.dataframe(summary[['total', 'available', 'PnL_pc', 'average_buy_price', 'price',
                      'PnL_tt', 'average_buy_price_usd', 'price_usd']]
             .style.applymap(foreground_by_sign, subset=['PnL_pc', 'PnL_tt']))

st.text("last completed transactions:")
transactions = wallet.get_transaction()[['base_currency', 'time', 'side', 'price', 'size']]
transactions.set_index('base_currency', inplace=True)
st.dataframe(transactions.head(8))

logger.info('--------------------- Wired Exchange Terminated ---------------------')
