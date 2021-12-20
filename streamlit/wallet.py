import os
from dotenv import load_dotenv

import wired_exchange.core
from wired_exchange.binance import BinanceClient
from wired_exchange.portfolio import Portfolio
import streamlit as st

import glob

def get_profiles():
    return [os.path.basename(p.split('_')[0]) for p in glob.glob('./*.sqlite')]


def foreground_by_sign(v):
    return 'color: red' if v < 0 else 'color: green' if v > 0 else ""


def import_all(wallet: Portfolio):
    wallet.import_account_operations()
    wallet.import_transactions()


profile = st.selectbox("select portfolio", ['EBL', 'POL'])

load_dotenv(f'.env-{profile}', override=True)

if profile == 'EBL':
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

    st.text("last orders:")
    orders = wallet.get_orders()[['base_currency', 'side', 'price', 'size', 'status', 'time']]
    orders.set_index('base_currency', inplace=True)
    # transactions = wallet.get_transaction()[['base_currency', 'time', 'side', 'price', 'size']]
    # transactions.set_index('base_currency', inplace=True)
    st.dataframe(orders.head(15))

    st.text("futures positions:")
    futures = wallet.get_futures()[['symbol', 'markPrice', 'realisedPnl', 'avgEntryPrice', 'unrealisedPnlPcnt', 'realLeverage', 'openingTimestamp', 'liquidationPrice']]
    # orders.set_index('base_currency', inplace=True)
    # transactions = wallet.get_transaction()[['base_currency', 'time', 'side', 'price', 'size']]
    # transactions.set_index('base_currency', inplace=True)
    st.dataframe(futures.head(15))

else:
    if profile == 'POL':
        st.title(f'{profile} Wallet')
        binance = BinanceClient(os.getenv('binance_api_key'), os.getenv('binance_api_secret'))
        st.text("Positions:")
        balances = binance.get_balances()
        st.dataframe(balances[(balances['total'] > .0001) & (balances.index != 'USDT') & (balances.index != 'USD')])
        st.text("last orders:")
        transactions = binance.get_transactions()[['base_currency', 'side', 'price', 'size', 'amount', 'status', 'time']]
        transactions.set_index('base_currency', inplace=True)
        st.dataframe(transactions.head(15))

st.text(f'wired_exchange v{wired_exchange.core.VERSION}')