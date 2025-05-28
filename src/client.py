import json
import asyncio
import logging
import websockets
from typing import Callable
from websockets.asyncio.client import connect
from systematic.engine.config import NetworkConfig
from systematic.engine.utils import build_message

logger = logging.getLogger(__name__)


class WebsocketClient:

    AVAILABLE_PRIVATE_TOPICS = ("trading",)
    AVAILABLE_PUBLIC_TOPICS = ("quotes", "trades", "lobviz")

    def __init__(self, on_trade: Callable = print):
        host = NetworkConfig.host
        port = NetworkConfig.port
        uri = f"ws://{host}:{port}"
        self._public_uri = f"{uri}/public"
        self._private_uri = f"{uri}/private"
        self._client_id = None
        self.tasks = {}

    async def unsubscribe(self, topic):
        """Cancels any running task of a given `topic`

        Parameters
        ----------

        topic: str
            One of the available topics:
            - orders
            - trades
            - lobviz
            - quotes
        """
        logger.debug(f"Cancelling {topic=}")
        self.tasks[topic].cancel()
        while not self.tasks[topic].cancelled():
            await asyncio.sleep(0)

    async def subscribe(self, topic, callback):
        assert (
            topic in self.AVAILABLE_PUBLIC_TOPICS
            or topic in self.AVAILABLE_PRIVATE_TOPICS
        )

        t = self.tasks
        if topic in t and not (t[topic].done() or t[topic].cancelled()):
            logger.warning(f"Already subscribed. check {t[topic]}")
        else:
            logger.info(f"Subscribing to {topic=}")
            if topic == "trading":
                t[topic] = asyncio.create_task(
                    self._subscribe_private(callback), name=f"{topic}_task"
                )
            else:
                t[topic] = asyncio.create_task(
                    self._subscribe_impl(topic, callback), name=f"{topic}_task"
                )
            await asyncio.sleep(0)

    async def place_order(self, **kwargs):
        if self._client_id is None:
            raise ValueError(
                "Cannot place orders. You need to subscribe to trading."
            )
        kwargs["client_id"] = self._client_id
        message = build_message("trade", params=kwargs)
        logger.info(f"Sending trade {message=}")
        async with connect(self._private_uri) as ws:
            await ws.send(message)

    async def _subscribe_impl(self, topic, callback):
        callback = callback or print
        async for websocket in connect(self._public_uri):
            logger.debug(f"Connected to {self._public_uri} for {topic=}")
            try:
                logger.debug(f"Creating new websocket {websocket.id!s}")
                message = build_message(event=topic)
                await websocket.send(message)
                while True:
                    message = json.loads(await websocket.recv())
                    callback(message)
            except websockets.ConnectionClosed:
                logger.warning("Public connection lost. Server disconnected")
            except Exception as e:
                raise e

    async def _subscribe_private(self, callback):
        async for websocket in connect(self._private_uri):
            logger.debug(f"Connected to URI: {self._private_uri}")
            try:
                init_message = build_message(event="init")
                await websocket.send(init_message)
                self._client_id = await websocket.recv()
                while True:
                    message = json.loads(await websocket.recv())
                    logger.debug(f"Received {message=}")
                    callback(message)
            except websockets.ConnectionClosed:
                logger.warning("Private connection lost. Server disconnected")
