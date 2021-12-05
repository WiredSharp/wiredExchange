import logging
import os
from logging.config import fileConfig
from dotenv import load_dotenv
from wired_exchange.portfolio import Portfolio
import streamlit as st
import pandas as pd

load_dotenv()

logging.config.fileConfig('logging.conf')

logger = logging.getLogger('main')
logger.info('--------------------- starting Wired Exchange ---------------------')
# print(f"PYTHONPATH: {os.getenv('PYTHONPATH')}")
wallet = Portfolio('EBL')
st.title('EBL Wallet')
st.write(wallet.get_summary())
logger.info('--------------------- starting Wired Exchange ---------------------')
