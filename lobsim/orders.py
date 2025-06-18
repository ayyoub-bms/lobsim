from __future__ import annotations
import logging
import uuid
from enum import Enum
from dataclasses import dataclass
from typing import Self, Dict, TypeAlias

from lobsim.utils import now
from lobsim.utils import Timestamp
from lobsim.utils import exist_any
from lobsim.instruments import Instrument

logger = logging.getLogger(__name__)

Queue: TypeAlias = "Queue"


class OrderType(Enum):
    CANCEL = "CANCEL"
    AMEND = "AMEND"
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    MARKETABLE = "LIMIT"
    STOP = "STOP"
    STOP_MARKET = "STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"

    def __str__(self):
        return self.name


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"
    GTD = "GTD"


class Side(Enum):
    BID = 1
    ASK = -1

    @property
    def is_bid(self) -> bool:
        return self == Side.BID

    @staticmethod
    def lob_side(order_side: str):
        assert order_side in ("Buy", "Sell"), f"Unknown {order_side=}"
        return -1 if order_side == "Sell" else 1

    @staticmethod
    def from_str(side: str):
        if side == "BID":
            return Side.BID
        return Side.ASK

    def __neg__(self) -> Self:
        if self == Side.BID:
            return Side.ASK
        return Side.BID

    def __mul__(self, x: float) -> float:
        return self.value * x

    __rmul__ = __mul__

    def __str__(self):
        return self.name


@dataclass
class Trade:
    trade_id: str
    instrument: Instrument
    side: Side
    price: float
    quantity: float
    engine_ts: Timestamp = now()


@dataclass
class Fill:
    order_id: str
    price: float
    quantity: float
    created: Timestamp = now()

    def __str__(self):
        return f"Fill(price={self.price}, "
        f"quantity={self.quantity}, engine_ts={self.created})"


@dataclass
class Order:
    owner: str
    instrument: Instrument
    side: Side
    quantity: float
    price: float

    def __post_init__(self):
        engine_ts = now()
        # reference to the previous order
        self.oprev: Self = None
        # reference to the next order
        self.onext: Self = None
        # reference to the orders' own queue
        self.queue: Queue = None
        # timestamp of the moment the order has been updated
        self.updated: Timestamp = engine_ts
        # timestamp of the moment theorder has been created
        self.created: Timestamp = engine_ts
        self.order_id: str = str(uuid.uuid4())
        # remaining quantity after fills have been added
        self.remaining: float = self.quantity
        self.last_filled_quantity = 0

    @property
    def filled(self):
        """Returns True if the order as been filled"""
        return self.remaining == 0

    def infos(self) -> Dict:
        """Provides the following data as a dictionnary:
        - The order ID
        - The side of the order: Bid or ask
        - The order size
        - The remaining quantity
        - The limit price
        - The timestamp when the order has been created.
        - The timestamp when the order has been updated.
        """
        return dict(
            order_id=self.order_id,
            side=str(self.side),
            quantity=self.quantity,
            remaining=self.remaining,
            price=self.price,
            created=self.created,
            updated=self.updated,
        )

    def update(self, price=None, quantity=None, queue=None):
        """Updates any of the price, quantity or the queue of the order.
        This mainly occurs when amending an order by the user.

        Parameters
        ----------

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`

        price: float
            The price of the limit order at wich the buy or sell order
            will be executed.
            The price should live in the tick-grid,
            i.e a multiple of `tick_size`

        queue: systematic.engine.orderbook.Queue
            The queue at wich the order belongs

        """
        logger.debug(
            f"Updating order {self}: {quantity=}, {price=}, queue={queue!r}"
        )
        if price is not None:
            self.price = price

        if quantity is not None:
            self.quantity = quantity
            self.remaining = quantity

        if queue is not None:
            self.queue = queue

        if exist_any(price, quantity, queue):
            self.updated = now()

    def add_fill(self, quantity: float) -> Fill:
        """Adds a new fill upon a (partial) execution of the order.

        Parameters
        ----------

        quantity: float
            The size of the fill as a multiple of `step_size`.
            The quantity should be between `min_qty` and `max_qty`
        """
        update_ts = now()
        logger.debug(f"Adding Fill({quantity=})")
        fill = Fill(self.order_id, self.price, quantity, update_ts)
        self.last_filled_quantity = quantity
        v_precision = self.instrument.precision.quote_precision
        self.remaining = round(self.remaining - quantity, v_precision)
        self.updated = update_ts
        logger.info(f"Added Fill({quantity=}) to {self}")
        return fill

    def __str__(self):
        odata = self.infos()
        params = ", ".join(f"{k}={str(odata[k])}" for k in self.infos())
        return f"Order({params})"

    def __eq__(self, other: Self):
        return self.order_id == other.order_id
