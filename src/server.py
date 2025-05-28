import logging
import signal
import json
import asyncio
import websockets
from functools import partial
from systematic.engine.orderbook import Orderbook
from systematic.engine.orders import OrderType
from systematic.engine.orders import Side
from systematic.engine.config import NetworkConfig
from systematic.engine.config import ExchangeConfig
from systematic.engine.utils import build_message
from systematic.engine.utils import _PubSub
from systematic.markets.instruments.instrument import Instrument

logger = logging.getLogger(__name__)


class WebsocketServer:

    def __init__(
        self,
        instrument: Instrument,
        network_config: NetworkConfig = NetworkConfig(),
        exchange_config: ExchangeConfig = ExchangeConfig(),
        client_timeout: int = 10,
    ):

        self.tasks = {}
        self._public_chanel = {}
        self._private_chanel = {}

        self.port = network_config.port
        self.host = network_config.host

        self._trades_freq = exchange_config.trades_freq
        self._quotes_freq = exchange_config.quotes_freq

        self.client_timeout = client_timeout
        self._orderbook = Orderbook(
            instrument=instrument, send_private=self._private_broadcast
        )

        self._add_signal_handlers()

    def public_chanel(self, key: str):
        if key not in self._public_chanel:
            self._public_chanel[key] = _PubSub()
        return self._public_chanel[key]

    def private_chanel(self, key: str):
        if key not in self._private_chanel:
            self._private_chanel[key] = _PubSub()
        return self._private_chanel[key]

    async def start(self):
        try:
            async with websockets.serve(self._start, self.host, self.port):
                # start broadcasting public feed to clients
                async with asyncio.TaskGroup() as tg:
                    logger.info("Starting Quotes stream")
                    tg.create_task(self._quotes_stream(), name="quote_task")
                    logger.info("Starting Trades stream")
                    tg.create_task(self._trades_stream(), name="trade_task")
                    logger.info("Starting lobviz stream")
                    tg.create_task(self._lobviz_stream(), name="lobviz_task")
                logger.warning("Terminated public feed")
        except asyncio.CancelledError:
            logger.warning("Stopping the server, Good Bye!")

    def reset(self):
        for task in asyncio.tasks.all_tasks():
            task.cancel("Server shutdown")
        self._public_chanel.clear()
        self._private_chanel.clear()

    async def _lobviz_stream(self):
        topic = "lobviz"
        while True:
            try:
                message = build_message(
                    event=topic, data=dict(lob=str(self._orderbook))
                )
                self.public_chanel(topic).publish(message)
                await asyncio.sleep(self._quotes_freq)
            except asyncio.CancelledError:
                logger.warning("Cancelling lobviz stream")
                break

    async def _quotes_stream(self):
        topic = "quotes"
        while True:
            try:
                lob_state = self._orderbook.get_state()
                message = build_message(event=topic, data=lob_state)
                self.public_chanel(topic).publish(message)
                await asyncio.sleep(self._quotes_freq)
            except asyncio.CancelledError:
                logger.warning("Cancelling quotes stream")
                break

    async def _trades_stream(self):
        topic = "trades"
        while True:
            try:
                message = build_message(
                    event=topic, message="Not yet implemented"
                )
                self.public_chanel(topic).publish(message)
                await asyncio.sleep(self._trades_freq)
            except asyncio.CancelledError:
                logger.warning("Cancelling trades stream")
                break

    async def _send_custom_ping(self, websocket):
        client_id = str(websocket.id)
        while True:
            try:
                await asyncio.sleep(self.client_timeout)
                await websocket.ping(client_id)
            except websockets.ConnectionClosed:
                logger.debug(f"{client_id=} disconnected. Cleaning up.")
                self._clean_private(
                    client_id, err_msg="Client disconnected."
                )
                break

    async def _start(self, websocket):
        client_id = str(websocket.id)
        try:
            path = websocket.request.path
            logger.info(f"Incoming request through {path=}")
            async for message in websocket:
                message = json.loads(message)
                logger.debug(f"Received: {message=}")
                match path:
                    case "/private":
                        await self._on_private(websocket, message)

                    case "/public":
                        await self._on_public(websocket, message)

                    case _:
                        await self._on_error(websocket, f"Unknown {path=}")
        except websockets.ConnectionClosed:
            logger.warning(f"{client_id=} disconnected.")

    def _clean_private(self, client_id: str, err_msg: str = ""):
        if client_id in self._private_chanel:
            del self._private_chanel[client_id]
            logger.debug(f"Data cleanup for {client_id=} Done.")
        else:
            logger.debug(f"Nothing to clean for {client_id=}")

    async def _on_error(self, websocket, message):
        logger.error(message)
        message = build_message(event="error", message=message)
        websocket.send(message)

    async def _on_public(self, websocket, message):
        event_type = message["event"]
        logger.info(f"Listening for {event_type} events.")
        async for message in self.public_chanel(event_type):
            await websocket.send(message)

    async def _on_private(self, websocket, message):
        event_type = message["event"]
        client_id = str(websocket.id)
        match event_type:
            case "init":
                await self._on_trading_init(websocket)
            case "trade":
                params = message.get("params")
                if params is None:
                    await self._on_error(
                        websocket,
                        f"Missing data from {client_id=}. {message=}",
                    )
                else:
                    await self._on_trading_request(params)

            case _:
                await self._on_error(client_id, f"Unknown {event_type=}")

    async def _on_trading_init(self, websocket):
        client_id = str(websocket.id)
        await websocket.send(client_id)
        asyncio.create_task(
            self._send_custom_ping(websocket)
        )
        async for message in self.private_chanel(client_id):
            await websocket.send(message)

    async def _on_trading_request(self, params):
        logger.debug(f"Received {params=}")
        order_type = OrderType(params["order_type"])
        del params["order_type"]
        if "side" in params:
            params["side"] = Side(params["side"])
        logger.debug(f"Placing a {order_type!s} order with {params=}")
        match order_type:
            case OrderType.LIMIT:
                self._orderbook.on_limit(**params)
            case OrderType.AMEND:
                self._orderbook.on_amend(**params)
            case OrderType.MARKET:
                self._orderbook.on_market(**params)
            case OrderType.CANCEL:
                self._orderbook.on_cancel(**params)
            case OrderType.MARKETABLE:
                self._orderbook.on_marketable(**params)
            case _:
                logger.error(f"Unknown {order_type=}")

    def _private_broadcast(self, client_id, message):
        message = build_message(event="private", data=message)
        logger.debug(f"Publishing {message=} to {client_id=}")
        self.private_chanel(client_id).publish(message)
        logger.debug(f"Published {message=} to {client_id=}")

    def _add_signal_handlers(self):

        def catch_interruptions(signal, loop):
            logger.warning(f"Received {signal=}. Shutting down the server.")
            self.reset()
            loop.stop()

        event_loop = asyncio.get_event_loop()
        signals = (signal.SIGQUIT, signal.SIGINT, signal.SIGTERM)
        for sig in signals:
            callback = partial(catch_interruptions, sig, event_loop)
            event_loop.add_signal_handler(sig, callback)
