import logging
from dataclasses import dataclass
from typing import Self, Callable, TypeAlias
from termcolor import colored

from systematic.engine.orders import Side

logger = logging.getLogger(__name__)

# Forward reference to the order object
Order: TypeAlias = "Order"


@dataclass
class Queue:
    limit: float
    side: Side
    notify: Callable
    volume_precision: int = 2

    def __post_init__(self):
        self.qprev: Self = None
        self.qnext: Self = None
        self.ohead: Order = None
        self.otail: Order = None
        self.volume: float = 0.0
        self.nb_orders: int = 0

    @property
    def empty(self) -> bool:
        return self.volume == 0

    def add(self, order: Order):
        """Adds an `order` to the queue"""
        logger.debug(f"Adding {order=!s}")
        if self.ohead is None:
            self.ohead = order
            self.otail = order
        else:
            order.oprev = self.otail
            order.onext = None  # I am the last order of the Q
            self.otail.onext = order
            self.otail = order

        self.volume = self._rv(self.volume + order.quantity)
        self.nb_orders += 1
        order.update(queue=self)
        self._notify(status="New order", order=order)
        logger.info(f"Added {order=!s}")

    def remove(self, order: Order):
        """Removes the `order` from the queue"""
        logger.debug(f"Removing {order=!s}")
        self.nb_orders -= 1
        # Either the order has been filled and is marked for deletion
        # Or the order is still live and is being cancelled.
        # In the latter case the remaining quantity needs to be reduced
        # from the queue volume. While in the former case, the remaning
        # volume to be deduced is nothing but the last filled quantity
        if order.filled:
            self.volume = self._rv(self.volume - order.last_filled_quantity)
        else:
            self.volume = self._rv(self.volume - order.remaining)

        # If I am the only order in the queue
        if order == self.ohead and order == self.otail:
            logger.debug("Removing The only order of the queue")
            self.ohead = None
            self.otail = None

        # If I am the tail of the queue
        elif order == self.otail:
            logger.debug("Removing the tail order")
            self.otail = order.oprev
            order.oprev.onext = None

        # If I am the head of the queue
        elif order == self.ohead:
            logger.debug("Removing the head order")
            self.ohead = order.onext
            order.onext.oprev = None
        else:
            # If I am in the middel of the queue
            logger.debug("Removing the order from inside the queue")
            order.onext.oprev = order.oprev
            order.oprev.onext = order.onext

        logger.info(f"Removed {order=!s} from {self!r}")

    def fill(self, order: Order, quantity: float):
        """Fills an `order` with `quantity`.
        If the order is filled then the order is removed from the queue.
        Otherwise its size is reduced by `quantity`.

        Parameters
        ----------

        quantity: float
            The filled quantity as a multiple of `step_size`.
            The quantity should be between `min_qty` and `max_qty`
        """
        fill = order.add_fill(quantity)
        message = dict(status="New Fill", fill=str(fill))
        if self.notify is not None:
            self.notify(client_id=fill.order_id, message=message)
        if order.filled:
            self._notify("Filled", order)
        else:
            self._notify("Partial fill", order)
            logger.debug(
                f"Reducing the queue volume from {self.volume}"
                f"to {self.volume - quantity}"
            )
            self.volume = self._rv(self.volume - quantity)

    def _notify(self, status: str, order: Order):
        if self.notify is not None:
            message = dict(status=status)
            message.update(order.infos())
            self.notify(order.owner, message)

    def _rv(self, quantity):
        return round(quantity, self.volume_precision)

    def __str__(self):
        return colored(
            "▮" * self.nb_orders, "red" if self.side.is_bid else "green"
        )

    def __repr__(self):
        return f"Queue(side={self.side}, limit={self.limit}, volume={self.volume})"

    def __eq__(self, other: Self) -> bool:
        return self.limit == other.limit
