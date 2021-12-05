import asyncio
import logging
import random
from uuid import uuid4

import websockets

from wired_exchange.kucoin import CandleStickResolution

WS_OPEN_TIMEOUT = 10
WS_CONNECTION_TIMEOUT = 3


class WebSocketMessageHandler:

    def can_handle(self, message: str) -> bool:
        pass

    def handle(self, message: str) -> bool:
        """process received message and indicates if handler must be kept registered
        one time handler are useful when waiting for acknowledgement"""
        pass


class KucoinWebSocket:

    def __init__(self, endpoint, token, encrypt: bool,
                 ping_interval: int, ping_timeout: int, connect_id: str = None):
        self._encrypt = encrypt
        self._ping_timeout = ping_timeout
        self._ping_interval = ping_interval
        self._endpoint = endpoint
        self._token = token
        self._id = connect_id if connect_id is not None else str(uuid4()).replace('-', '')
        self._logger = logging.getLogger(type(self).__name__)
        self._ws = None
        self._welcomeHandler = self.WelcomeMessageHandler()
        self._handlers = [self._welcomeHandler, SinkMessageHandler()]
        self._opening = asyncio.Event()

    async def read_async(self, topics: list[tuple[str, str, CandleStickResolution]]):
        uri = f"{self._endpoint}?token={self._token}&connectId={self._id}"
        try:
            async for ws in websockets.connect(uri,
                                               logger=self._logger,
                                               ssl=self._encrypt,
                                               open_timeout=WS_OPEN_TIMEOUT,
                                               ping_interval=self._ping_interval,
                                               ping_timeout=self._ping_timeout):
                try:
                    self._ws = ws
                    self._disconnect()
                    self.insert_handler(self.WelcomeMessageHandler())
                    await self.subscribe_klines_async(topics)
                    async for message in ws:
                        try:
                            self.handle_message(message)
                        except:
                            self._logger.error(f'something goes wrong when processing message: {message}')
                        return
                except websockets.ConnectionClosed:
                    continue
        finally:
            self._disconnect()
            self._ws = None

    def insert_handler(self, handler: WebSocketMessageHandler):
        self._handlers.insert(handler)

    def handle_message(self, message):
        self._logger.debug(f'message received: {message}')
        for handler in self._handlers:
            if handler.can_handle(message):
                handler.handle(message)
                return

    async def subscribe_klines_async(self, topics: list[tuple[str, str, CandleStickResolution]]):
        await self._wait_connection_async()
        subscription_id = random.randint(100000000, 1000000000)
        self.insert_handler(KlineSubscriptionHandler(subscription_id))
        self._ws.send(self.new_klines_subscription_message(subscription_id, topics))

    def new_klines_subscription_message(self, subscription_id: int,
                                        topics: list[tuple[str, str, CandleStickResolution]]):
        return f"""
        {
        "id": {subscription_id},
            "type": "subscribe",
            "topic": "/market/candles:{','.join([f'{bc}-{qc}_{res.value}' for bc, qc, res in topics])}",
            "response": true
        }
        """

    def _disconnect(self):
        self._welcomeHandler.release()

    def _wait_connection_async(self, timeout: int = WS_CONNECTION_TIMEOUT):
        return self._welcomeHandler.wait_connection_async(timeout)

    class WelcomeMessageHandler(WebSocketMessageHandler):
        def __init__(self):
            self._connected = asyncio.Event()

        def can_handle(self, message):
            return '"type":"welcome"' in message

        def handle(self, message):
            self._connected.set()
            return False

        def release(self):
            self._connected.clear()

        def is_connected(self):
            self._connected.is_set()

        def wait_connection_async(self, timeout: int):
            return asyncio.wait_for(self._connected.wait(), timeout)


class SinkMessageHandler(WebSocketMessageHandler):
    def __init__(self):
        self._connected = asyncio.Event()
        self._logger = logging.getLogger(type(self).__name__)

    def can_handle(self, message):
        return True

    def handle(self, message):
        self._logger.debug(f'unhandled message received: {message}')
        return True

    def wait_connection_async(self, timeout: int):
        return asyncio.wait_for(self._connected.wait(), timeout)


class KlineSubscriptionHandler(WebSocketMessageHandler):
    def __init__(self, subscription_id: int):
        self.subscription_id = subscription_id
        self._logger = logging.getLogger(type(self).__name__)

    def can_handle(self, message: str) -> bool:
        return f'"id":{self.subscription_id}' in message

    def handle(self, message: str) -> bool:
        if '"type":"ack"' in message:
            self._logger.info(f'subscription #{self.subscription_id} acknowledged')
        else:
            self._logger.warning(f'subscription #{self.subscription_id}: unexpected response: {message}')
        return False


class StrategyHandler(WebSocketMessageHandler):
    def __init__(self, subscription_id: int):
        self.subscription_id = subscription_id
        self._logger = logging.getLogger(type(self).__name__)

    def can_handle(self, message: str) -> bool:
        return f'"id":{self.subscription_id}' in message

    def handle(self, message: str) -> bool:
        if '"type":"ack"' in message:
            self._logger.info(f'subscription #{self.subscription_id} acknowledged')
        else:
            self._logger.warning(f'subscription #{self.subscription_id}: unexpected response: {message}')
        return False
