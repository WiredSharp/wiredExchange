import asyncio
import json
import shutil
import signal
from datetime import datetime, timedelta
import logging
from logging.config import fileConfig

import tzlocal
from dotenv import load_dotenv
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
from kucoin.client import Market

import xlsxwriter

from wired_exchange import import_transactions, WiredStorage
from wired_exchange.core import read_transactions
from wired_exchange.ftx.FTXClient import FTXClient
from wired_exchange.kucoin import KucoinClient, CandleStickResolution
from wired_exchange.kucoin.WebSocket import WebSocketMessageHandler
from wired_exchange.portfolio import Portfolio

from typing import Union


async def monitor_tasks():
    while True:
        tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
        ]
        [t.print_stack(limit=5) for t in tasks]
        await asyncio.sleep(2)


def get_or_create_eventloop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError as ex:
        if "There is no current event loop in thread" in str(ex):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return asyncio.get_event_loop()


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

# signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
# for s in signals:
#     loop.add_signal_handler(
#         s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
#     )

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks)
    loop.stop()


async def main(kucoin):
    # Not supported under windows
    # May want to catch other signals too
    # signals = (signal.SIGTERM, signal.SIGINT)
    # for s in signals:
    #     loop.add_signal_handler(
    #         s, lambda s=s: asyncio.create_task(shutdown(s, loop)))
    tasks = [asyncio.create_task(monitor_tasks()),
             asyncio.create_task(kucoin.read_topics_async([('AVAX', 'USDT', CandleStickResolution.MIN_1)]))]
    await asyncio.gather(*tasks)


class MyCandleStrategy(WebSocketMessageHandler):
    def __init__(self, topics: list[(str, str, CandleStickResolution)]):
        self._topics = topics
        self._logger = logging.getLogger(type(self).__name__)
        self._topics_pattern = [f'"topic":"/market/candles:{bc}-{qc}_{res.value}"' for bc, qc, res in self._topics]

    # {"type":"message","topic":"/market/candles:AVAX-USDT_1min","subject":"trade.candles.update","data":{"symbol":"AVAX-USDT","candles":["1638911040","90.11","89.954","90.11","89.938","441.7103","39779.5127417"],"time":1638911078201789417}}
    def can_handle(self, message: str) -> bool:
        for candle in self._topics_pattern:
            if candle in message:
                return True
        return False

    def handle(self, message: str):
        self._logger.info(f'processing {message}')

    @property
    def topics(self):
        return self._topics


class MyTickerStrategy(WebSocketMessageHandler):
    def __init__(self, tickers: Union[list[(str, str)], None]):
        self._tickers = tickers
        self._logger = logging.getLogger(type(self).__name__)
        if tickers is None:
            self._tickers_pattern = [f'"topic":"/market/ticker:']
        else:
            self._tickers_pattern = [f'"topic":"/market/ticker:{bc}-{qc}"' for bc, qc in self._tickers]

    # {"type":"message","topic":"/market/tickers:AVAX-USDT_1min","subject":"trade.candles.update","data":{"symbol":"AVAX-USDT","candles":["1638911040","90.11","89.954","90.11","89.938","441.7103","39779.5127417"],"time":1638911078201789417}}
    def can_handle(self, message: str) -> bool:
        for ticker in self._tickers_pattern:
            if ticker in message:
                return True
        return False

    def handle(self, message: str):
        self._logger.info(f'processing ticker {message}')

    @property
    def tickers(self):
        return self._tickers


async def scenario(kucoin: KucoinClient):
    await kucoin.register_candle_strategy_async(MyCandleStrategy(
        [('AVAX', 'USDT', CandleStickResolution.MIN_1), ('MANA', 'USDT', CandleStickResolution.MIN_1)]))
    logger.info('first strategy registered')
    await kucoin.register_ticker_strategy_async(MyTickerStrategy(None))
    logger.info('second strategy registered')
    await asyncio.sleep(15)
    kucoin.stop_reading()


if __name__ == "__main__":
    wallet = Portfolio('EBL')
    print(wallet.get_orders())
    # wallet.import_transactions(start_time=datetime.now(tzlocal.get_localzone()) - timedelta(days=7))
    # print(wallet.get_summary()['average_buy_price_usd'])

    # with KucoinClient() as kucoin:
    #     print(kucoin.get_orders(status='active', start_time= datetime.fromisoformat('2021-11-10T21:53:00+01:00')))
    #
    # with FTXClient() as ftx:
    #     print(ftx.get_orders())
    #     # loop = get_or_create_eventloop()
    #     # asyncio.run(monitor_tasks())
    #     # asyncio.run(kucoin.read_topics_async([('AVAX','USDT', CandleStickResolution._1min)]), debug=True)
    #     # asyncio.run(kucoin.register_strategy_async(MyStrategy([('AVAX', 'USDT', CandleStickResolution._1min)])))
    #     # print(kucoin.get_prices_history('AVAX', 'USDT', CandleStickResolution.to_seconds(CandleStickResolution.HOUR_4),
    #     #                           datetime.fromisoformat('2021-11-10T21:53:00+01:00')))
    #
    #     print(kucoin.get_orders(start_time=datetime.now(tzlocal.get_localzone()) - timedelta(days=40)))
        # print(kucoin.get_orders())
        # asyncio.run(scenario(kucoin))
    # loop = asyncio.get_running_loop()
    # loop.set_debug(True)
    # loop.create_task(monitor_tasks())
    # loop.create_task(kucoin.read_topics_async([('AVAX', 'USDT', CandleStickResolution._1min)]))
    # loop.run_forever()
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

# wallet = Portfolio('EBL')
#
# wallet.import_account_operations(datetime.fromisoformat('2021-11-10T18:53:00+01:00'))
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
