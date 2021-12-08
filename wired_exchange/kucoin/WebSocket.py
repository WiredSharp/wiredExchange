import asyncio
import logging
import random
from enum import Enum
from uuid import uuid4

import websockets

from wired_exchange.kucoin import CandleStickResolution

WS_OPEN_TIMEOUT = 10
WS_CONNECTION_TIMEOUT = 3


class WebSocketState(Enum):
    STATE_WS_READY = 1
    STATE_WS_CLOSING = 2


class WebSocketNotification(Enum):
    CONNECTION_LOST = 1


class WebSocketMessageHandler:

    def can_handle(self, message: str) -> bool:
        pass

    def handle(self, message: str) -> bool:
        """process received message and indicates if handler must be kept registered
        one time handler are useful when waiting for acknowledgement"""
        pass

    def on_notification(self, notification: WebSocketNotification):
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
        self._connected = asyncio.Event()
        self._handlers = [PongMessageHandler(self, self._ping_interval, self._ping_timeout),
                          self.WelcomeMessageHandler(self._connected), SinkMessageHandler()]
        self._state: WebSocketState = WebSocketState.STATE_WS_READY

    async def open_async(self):
        uri = f"{self._endpoint}?token={self._token}&connectId={self._id}"
        try:
            if self._state != WebSocketState.STATE_WS_READY:
                return
            async for ws in websockets.connect(uri,
                                               logger=self._logger,
                                               ssl=self._encrypt,
                                               open_timeout=WS_OPEN_TIMEOUT,
                                               ping_interval=self._ping_interval,
                                               ping_timeout=self._ping_timeout):
                try:
                    if self._state == WebSocketState.STATE_WS_CLOSING:
                        break
                    self._ws = ws
                    self._disconnect()
                    await self._run_message_loop(ws)
                except websockets.ConnectionClosed:
                    continue
        finally:
            for handler in self._handlers:
                handler.on_notification(WebSocketNotification.CONNECTION_LOST)
            self._disconnect()
            self._ws = None
            self._state = WebSocketState.STATE_WS_READY

    async def _run_message_loop(self, ws: websockets):
        async for message in ws:
            try:
                if self._state == WebSocketState.STATE_WS_CLOSING:
                    break
                self._handle_message(message)
            except:
                self._logger.error(f'something goes wrong when processing message: {message}')

    def insert_handler(self, handler: WebSocketMessageHandler):
        self._handlers.insert(0, handler)
        self._logger.debug(f'{type(handler).__name__}: handler registered')

    def _handle_message(self, message):
        for handler in self._handlers:
            if handler.can_handle(message):
                self._logger.debug(f'handler found: {type(handler).__name__}')
                handler.handle(message)
                return

    async def subscribe_klines_async(self, topics: list[tuple[str, str, CandleStickResolution]]):
        try:
            await self.wait_connection_async()
            subscription_id = random.randint(100000000, 1000000000)
            self.insert_handler(KlineSubscriptionHandler(subscription_id))
            await self._ws.send(self._new_klines_subscription_message(subscription_id, topics))
            self._logger.debug('subscription completed')
        except TimeoutError:
            self._logger.error('subscription timeout', exc_info=True)

    def _new_klines_subscription_message(self, subscription_id: int,
                                         topics: list[tuple[str, str, CandleStickResolution]]):
        return f"""
        {{
        "id": {subscription_id},
            "type": "subscribe",
            "topic": "/market/candles:{','.join([f'{bc}-{qc}_{res.value}' for bc, qc, res in topics])}",
            "response": true
        }}
        """

    def _disconnect(self):
        self._connected.clear()
        return self

    def is_connected(self):
        self._connected.is_set()

    def wait_connection_async(self, timeout: int = WS_CONNECTION_TIMEOUT):
        return asyncio.wait_for(self._connected.wait(), timeout)

    def close(self):
        self._state = WebSocketState.STATE_WS_CLOSING

    async def send(self, message):
        await self.wait_connection_async()
        await self._ws.send(message)

    class WelcomeMessageHandler(WebSocketMessageHandler):
        def __init__(self, event: asyncio.Event):
            self._connected = event
            self._logger = logging.getLogger(type(self).__name__)

        def can_handle(self, message):
            return not self._connected.is_set() and '"type":"welcome"' in message

        def handle(self, message):
            self._connected.set()
            self._logger.debug('connection acknowledged by server')
            return True


class PongMessageHandler(WebSocketMessageHandler):
    def __init__(self, ws: KucoinWebSocket, ping_interval: int, ping_timeout: int):
        self._ws = ws
        self._ping_interval = ping_interval / 1000 * .95
        self._ping_timeout = ping_timeout / 1000
        self._task = asyncio.create_task(self._loop())
        self._task.set_name('ping_pong')
        self._pong = asyncio.Event()
        self._logger = logging.getLogger(type(self).__name__)

    async def _loop(self):
        while True:
            try:
                await self._send_ping_message()
                await asyncio.wait_for(self._pong.wait(), self._ping_timeout)
                self._pong.clear()
                await asyncio.sleep(self._ping_interval)
            except TimeoutError:
                self._logger.warning('ping timeout reached without pong')
                continue
            except asyncio.CancelledError:
                self._logger.warning('ping handler stopped')
                break

    def can_handle(self, message):
        return f'"type":"pong"' in message

    def handle(self, message):
        self._pong.set()
        return True

    def on_notification(self, notification: WebSocketNotification):
        if notification == WebSocketNotification.CONNECTION_LOST:
            self._task.cancel()

    def _send_ping_message(self):
        message_id = random.randint(100000000, 1000000000)
        return self._ws.send(f'{{ "id":{message_id},"type":"ping" }}')


class SinkMessageHandler(WebSocketMessageHandler):
    def __init__(self):
        self._logger = logging.getLogger(type(self).__name__)

    def can_handle(self, message):
        return True

    def handle(self, message):
        self._logger.debug(f'unhandled message received: {message}')
        return True


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
    pass
