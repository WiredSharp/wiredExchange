import asyncio
import pandas as pd
import sqlalchemy
from coingecko_api_client.client import CoinGeckoAPIAsyncClient
from sqlalchemy import Table, MetaData, select


async def ping_coingecko():
    coingecko = CoinGeckoAPIAsyncClient()
    try:
        # pis CoinGecko API with async client
        async_ping_data = await coingecko.ping()
        print(async_ping_data)
    finally:
        await coingecko.close()


async def load_coins_data():
    coingecko = CoinGeckoAPIAsyncClient()
    try:
        # pis CoinGecko API with async client
        coins = await coingecko.coins_list()
        df = pd.DataFrame(coins).set_index('symbol').rename(columns={'id': 'id_coingecko'})
        db = sqlalchemy.create_engine('sqlite:///wiredexchange.db')
        df.to_sql('COINS', db, if_exists='replace', index=True
                    , dtype={"id_coingecko": sqlalchemy.types.NVARCHAR(50),
                             "symbol": sqlalchemy.types.NVARCHAR(20),
                             "name": sqlalchemy.types.NVARCHAR(80)})
    finally:
        await coingecko.close()

async def load_exchanges_data():
    coingecko = CoinGeckoAPIAsyncClient()
    try:
        exchanges = await coingecko.exchanges_list()
        df = pd.DataFrame(exchanges).set_index('name').rename(columns={'id': 'id_coingecko'})
        db = sqlalchemy.create_engine('sqlite:///wiredexchange.db')
        df.to_sql('EXCHANGES', db, if_exists='replace', index=True
                    , dtype={"id_coingecko": sqlalchemy.types.NVARCHAR(50),
                             "name": sqlalchemy.types.NVARCHAR(80)})
    finally:
        await coingecko.close()

async def load_market_data(symbol):
    try:
        db = sqlalchemy.create_engine('sqlite:///wiredexchange.db')
        metadata = MetaData()
        coins = Table('COINS', metadata, autoload=True, autoload_with=db)
        id_coingecko = db.scalar(select(coins.c.id_coingecko).where(coins.symbol == symbol))
        coingecko = CoinGeckoAPIAsyncClient()
        raise RuntimeError('to be implemented')
    finally:
        await coingecko.close()
