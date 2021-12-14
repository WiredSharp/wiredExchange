import asyncio
import datetime
import json
import logging
from logging.config import fileConfig
from pathlib import Path
from msvcrt import getch

import pandas as pd
from dotenv import load_dotenv
from wired_exchange.kucoin import KucoinClient
from wired_exchange.core.ExchangeClient import ExchangeClient
from wired_exchange.kucoin.WebSocket import WebSocketMessageHandler, WebSocketNotification

from ta.trend import ema_indicator, macd, macd_diff
from ta.momentum import rsi
from ta.volatility import bollinger_hband, bollinger_lband, bollinger_mavg

import matplotlib.pyplot as plt

from typing import Union

load_dotenv()

logging.config.fileConfig('crypto_scanner.logging.conf')

logger = logging.getLogger('main')
logger.info('--------------------- starting Crypto Scanner ---------------------')


class RecorderMessageHandler(WebSocketMessageHandler):
    def __init__(self, strategy, output_file: str):
        self._inner = strategy
        self.output_file = output_file
        self.messages = []
        self._logger = logging.getLogger(type(self).__name__)

    def can_handle(self, message: str) -> bool:
        return self._inner.can_handle(message)

    def handle(self, message: str) -> bool:
        self.messages.append(message + '\n')
        return self._inner.handle(message)

    def on_notification(self, notification: WebSocketNotification):
        if notification == WebSocketNotification.CONNECTION_LOST:
            with open(self.output_file, 'a') as f:
                f.writelines(self.messages)
            self.messages.clear()
        try:
            self._inner.on_notification(notification)
        except:
            self._logger.warning('something goes wrong when notifying inner strategy', exc_info=True)

    @property
    def tickers(self):
        return self._inner.tickers

    @property
    def topics(self):
        return self._inner.topics


class BackTester:
    def __init__(self, strategy: WebSocketMessageHandler, messages: Union[list[str], str]):
        self._strategy = strategy
        self._messages = messages
        self._logger = logging.getLogger(type(self).__name__)

    def start(self):
        if type(self._messages) is str:
            with open(self._messages, 'r', newline='\n') as f:
                for message in f.readlines():
                    self._logger.debug(f'sending message: {message}')
                    if self._strategy.can_handle(message):
                        self._strategy.handle(message)


class CryptoScanner(WebSocketMessageHandler):
    def __init__(self, client: ExchangeClient = None,
                 tickers: Union[list[Union[tuple[str, str], str]], type(None)] = None,
                 depth: int = 26, resolution: int = 60,
                 output_folder: str = None):
        self._logger = logging.getLogger(type(self).__name__)
        self._prices = dict()
        self.depth = depth
        self._client = client
        self.resolution = resolution
        self.output_folder = output_folder

        if tickers is None:
            self._tickers = None
            self._tickers_pattern = [f'"topic":"/market/ticker:']
            self._logger.warning('registering for all tickers')
        else:
            self._tickers = [(bc, 'USDT') if type(bc) is str else (bc[0], bc[1]) for bc in tickers]
            self._tickers_pattern = [f'"topic":"/market/ticker:{bc}-{qc}"' for bc, qc in self._tickers]

    def can_handle(self, message: str) -> bool:
        for ticker in self._tickers_pattern:
            if ticker in message:
                return True
        return False

    def handle(self, message: str):
        self._process(json.loads(message))
        return True

    # {"bestAsk":"0.00000282","bestAskSize":"405.89","bestBid":"0.00000281",
    # "bestBidSize":"34788.22","price":"0.00000281","sequence":"1612886239695","size":"209.04","time":1638999912447}
    def _process(self, ticker: dict):
        symbol: str = ticker['topic'].split(':')[1]
        if symbol == 'all':
            symbol = ticker['subject']
        if not symbol.endswith('-USDT'):
            return
        row = pd.DataFrame([ticker['data']])
        row['time'] = pd.to_datetime(row['time'], unit='ms', utc=True)
        row['price'] = pd.to_numeric(row['price'])
        base_currency = symbol.split('-')[0]
        row['base_currency'] = base_currency
        row.drop(columns=["bestAsk", "bestAskSize", "bestBid", "bestBidSize", "sequence", "size"])
        prices = self._prices.get(symbol)
        if prices is None:
            if self._client is not None:
                start_time = row['time'][0] - datetime.timedelta(seconds=self.resolution * self.depth)
                prices_history = self._client.get_prices_history(base_currency, 'USDT', self.resolution, start_time)
                if len(prices_history) > 0:
                    prices_history.reset_index(inplace=True)
                    prices = prices_history[['time', 'base_currency', 'close']].rename(columns={'close': 'price'})
                    prices['price'] = pd.to_numeric(prices['price'])
                    prices.append(row, ignore_index=True)
                else:
                    prices = row
            else:
                prices = row
            self._prices[symbol] = prices
        else:
            prices = prices.append(row, ignore_index=True)
            self._prices[symbol] = prices
            if len(prices) > self.depth:
                self._run_computation(symbol, prices)
                # truncate to 2 * depth ?

    def _run_computation(self, symbol: str, prices: pd.DataFrame):
        # MACD default window size = 26
        prices['MACD'] = macd(prices['price'])
        prices['MACD_DIFF'] = macd_diff(prices['price'])
        # RSI default window size = 14
        prices['RSI'] = rsi(prices['price'])
        # BB default window size = 20
        prices['BBG_H'] = bollinger_hband(prices['price'])
        prices['BBG_L'] = bollinger_lband(prices['price'])
        prices['BBG_M'] = bollinger_mavg(prices['price'])

    @property
    def tickers(self):
        return self._tickers

    def on_notification(self, notification: WebSocketNotification):
        if notification == WebSocketNotification.CONNECTION_LOST:
            if self.output_folder is not None:
                output_folder = Path(self.output_folder)
                output_folder.mkdir(exist_ok=True)
                for price in self._prices.items():
                    try:
                        price[1].to_json(output_folder / f'{price[0]}.json',
                                         orient='table', index=False, date_format='iso')
                        self._logger.debug(f'exporting {price[0]} prices')
                    except:
                        self._logger.warning(f'exporting {price[0]} prices', exc_info=True)


async def scenario(kucoin: KucoinClient):
    await kucoin.register_ticker_strategy_async(
        RecorderMessageHandler(CryptoScanner(kucoin, ['MNW', 'FTG', 'LINK'],
                                             output_folder='data/crypto_scanner'),
                               'data/ws_messages.txt'))
    await asyncio.sleep(20)
    kucoin.stop_reading()


def draw_graph():
    plt.figure(figsize=(10, 10))  # Create a figure containing a single axes.
    prices = pd.read_csv('data/LINK-USDT.csv', sep=';')
    plt.plot(prices['price'], label='price')
    plt.plot(prices['SMA200'], 'g--', label='SMA200')
    plt.plot(prices['EMA50'], 'r--', label='EMA50')
    plt.legend()
    # TODO
    # https://www.freecodecamp.org/news/algorithmic-trading-in-python/
    # plotting the sell signals
    # plt1.plot(signal_df.loc[signal_df.positions == -1.0].index, signal_df.short_mav[signal_df.positions == -1.0], 'v', markersize=10, color='g')
    # plotting the buy signals
    # plt1.plot(signal_df.loc[signal_df.positions == 1.0].index, signal_df.short_mav[signal_df.positions == 1.0], '^', markersize=10, color='r')
    plt.show()


def backtest():
    BackTester(CryptoScanner(), 'data/ws_messages.txt').start()


if __name__ == "__main__":
    # draw_graph()
    # backtest()
    with KucoinClient() as kucoin:
        asyncio.run(scenario(kucoin))

logger.info('--------------------- Wired Exchange Ended ---------------------')
