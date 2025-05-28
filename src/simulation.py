import logging

from typing import Callable, override

from systematic.markets.instruments import Instrument, instruments
from systematic.markets.exchanges.exchange import AsyncExchange
from systematic.engine.orders import Side
from systematic.engine.orders import OrderType
from systematic.engine.client import WebsocketClient

logger = logging.getLogger(__name__)


class VirtualExchange(AsyncExchange):

    name = "Simulation"

    def __init__(self, on_trade: Callable = print):
        self._instrument: Instrument = None
        self._client = WebsocketClient(on_trade=on_trade)

    def instrument(self, symbol):
        if self._instrument is None:
            self._instrument = instruments[symbol]
        return self._instrument

    @override
    async def subscribe_execution(self, callback=None):
        callback = callback or print
        topic = "trading"
        await self._client.subscribe(topic, callback)

    async def subscribe_lobviz(self, callback=None):
        """Register to have a hackish view of the lob.

        This visualization is here for testing purposes and is not meant
        to be used otherwise.

        Parameters
        ----------

        callback: Callable, default=print
            The function to call when a string representation of the lob is
            sent by the server. A default callback is set to print if it has
            None as value

        """
        callback = callback or print
        topic = "lobviz"
        await self._client.subscribe(topic, callback)

    @override
    async def subscribe_orderbook(
        self, symbol: str, depth: int = None, callback: Callable = None, *args
    ):
        """Listents to changes in the orderbook state.

        The events are received every n (ms) depending on the configuration
        set on the server.

        Parameters
        ----------

        callback: Callable, default=None
            The function to call when a new state of the lob is sent
            by the server.

        """
        topic = "quotes"
        await self._client.subscribe(topic, callback)

    @override
    async def subscribe_trades(
        self, symbol: str, callback: Callable = None, *args
    ):
        """Listents to new public transactions that occured in the past
        `m` milliseconds. Where `m` is set by the server.

        The events are received every n (ms) depending on the configuration
        set on the server.

        Parameters
        ----------

        callback: Callable, default=None
            The function to call when a new state of the lob is sent
            by the server.

        """
        topic = "trades"
        await self._client.subscribe(topic, callback)

    @override
    async def limit_order(
        self, *, symbol: str, side: str, quantity: float, price: float
    ):
        """Asynchronously send a limit order to the simulation server

        If the limit order crosses the best limits, then a marketable order is
        sent instead. See: `Orderbook.on_marketable` method.

        Parameters
        ---------

        side: str
            The side of order: Buy or Sell

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`

        price: float
            The price of the limit order at wich the buy or sell order
            will be executed.
            The price should live in the tick-grid, i.e a multiple of
            `tick_size`
        """
        instrument = self.instrument(symbol)

        if not instrument.is_valid_quantity(quantity):
            e = ValueError(f"Invalid {quantity=}")
            logger.exception(e)
            raise e

        if not instrument.is_valid_price(price):
            e = ValueError(f"Invalid {price=}")
            logger.exception(e)
            raise e

        await self._client.place_order(
            order_type=str(OrderType.LIMIT),
            side=Side.lob_side(side),
            quantity=quantity,
            price=price,
        )

    @override
    async def market_order(self, *, symbol: str, side: str, quantity: float):
        """Asynchronously sends a market order to the simulation server.

        Parameters
        ---------

        symbol: str
            The instrument symbol you want to trade
        side: Side
            The side of order: Buy or Sell

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`
        """
        instrument = self.instrument(symbol)
        if not instrument.is_valid_quantity(quantity):
            e = ValueError(f"Invalid {quantity=}")
            logger.exception(e)
            raise e

        await self._client.place_order(
            order_type=str(OrderType.MARKET),
            side=-Side.lob_side(side),
            quantity=quantity,
        )

    @override
    async def marketable_order(
        self, *, symbol: str, side: str, quantity: float, price: float
    ):
        """Asynchronously places a limit order that crosses the best limits.

        Parameters
        ---------
        side: Side
            The side of order: Buy or Sell

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`

        price: float
            The price of the limit order at wich the buy or sell order will
            be executed.
            The price should live in the tick-grid, i.e a multiple of
            `tick_size`
        """

        instrument = self.instrument(symbol)
        if not instrument.is_valid_quantity(quantity):
            e = ValueError(f"Invalid {quantity=}")
            logger.exception(e)
            raise e

        if not instrument.is_valid_price(price):
            e = ValueError(f"Invalid {price=}")
            logger.exception(e)
            raise e

        await self._client.place_order(
            order_type=str(OrderType.MARKETABLE),
            side=Side.lob_side(side),
            quantity=quantity,
            price=price,
        )

    @override
    async def cancel_order(self, *, order_id: str):
        """Asynchronously sends a cancels order to the simulation server

        Parameters
        ----------

        order_id: str
            The ID of the order to be canceled
        """
        await self._client.place_order(
            order_type=str(OrderType.CANCEL), order_id=order_id
        )

    @override
    async def amend_order(
        self,
        *,
        order_id: str,
        symbol: str,
        quantity: float = None,
        price: float = None,
    ):
        """Sends a request to the server to modify an already existing order.

        Parameters
        ----------

        order_id: str
            The ID of the order to be ameneded

        quantity: float
            The new order size as a multiple of `step_size`.
            The quantity should be between `min_qty` and `max_qty`

        price: float
            The new price of the limit order at wich the buy or sell order is
            to be executed.
            The price should live in the tick-grid, i.e a multiple of
            `tick_size`
        """
        instrument = self.instrument(symbol)
        params = dict(order_type=str(OrderType.AMEND), order_id=order_id)
        if quantity is not None:
            if instrument.is_valid_quantity(quantity):
                params["quantity"] = quantity
            else:
                e = ValueError(f"Invalid {quantity=}")
                logger.exception(e)
                raise e

        if price is not None:
            if instrument.is_valid_price(price):
                params["price"] = price
            else:
                e = ValueError(f"Invalid {price=}")
                logger.exception(e)
                raise e

        await self._client.place_order(**params)
