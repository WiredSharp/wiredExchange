import logging
import os
from logging.config import fileConfig
from dotenv import load_dotenv
from wired_exchange.portfolio import Portfolio
import streamlit as st
import pandas as pd
import glob

logging.config.fileConfig('logging.conf')


def get_profiles():
    return [os.path.basename(p.split('_')[0]) for p in glob.glob('./*.sqlite')]


logger = logging.getLogger('main')
logger.info('--------------------- starting Wired Exchange ---------------------')
profile = st.selectbox("select portfolio", get_profiles())
load_dotenv(f'.env-{profile}')
wallet = Portfolio(profile)
st.title(f'{profile} Wallet')
summary = wallet.get_summary()
summary['PnL_pc'] = (summary['average_price_usd'] - summary['average_buy_price_usd']) / summary['average_buy_price_usd'] * 100
summary['PnL_tt'] = (summary['average_price_usd'] - summary['average_buy_price_usd']) * summary['total']
st.dataframe(summary[['total', 'available', 'average_buy_price_usd', 'average_price_usd', 'PnL_pc', 'PnL_tt']])
logger.info('--------------------- Wired Exchange Terminated ---------------------')
