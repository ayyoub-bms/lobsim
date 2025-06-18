import logging
from typing import Dict
from typing import Callable
from typing import List
from termcolor import colored
from lobsim.utils import now
from lobsim.utils import is_divisible
from lobsim.utils import exist_any
from lobsim.queue import Queue
from lobsim.orders import Side
from lobsim.orders import Order
from lobsim.exceptions import OrderbookException
from lobsim.instruments import Instrument


logger = logging.getLogger(__name__)


class Orderbook:

    def __init__(self, instrument: Instrument, send_private: Callable = None):
        self.max_ask: float = 0.0
        self.min_bid: float = float("inf")
        self.best_queue: Dict[Side, Queue] = {Side.BID: None, Side.ASK: None}
        self.best_volumes: Dict[Side, float] = {Side.BID: 0, Side.ASK: 0}
        self.queues: Dict[float, Queue] = {}
        self.order_map: Dict[str, Order] = {}
        self._prev_mid = None
        self._curr_mid = None
        self._instrument = instrument
        self.send_private = send_private

    @property
    def tick_size(self):
        return self._instrument.price_details.tick_size

    @property
    def mid_price(self) -> float:
        return self._curr_mid

    def _update_mid(self):
        best_ask_q = self.best_queue[Side.ASK]
        best_bid_q = self.best_queue[Side.BID]
        if not exist_any(best_ask_q, best_bid_q):
            self._prev_mid = None
            self._curr_mid = None
            return

        if best_ask_q is None:
            self._curr_mid = self._instrument.adjust_price(
                best_bid_q.limit + 0.5 * self.tick_size
            )
            return

        if best_bid_q is None:
            self._curr_mid = self._instrument.adjust_price(
                best_ask_q.limit - 0.5 * self.tick_size
            )
            return

        self._prev_mid = self._curr_mid

        best_ask = best_ask_q.limit
        best_bid = best_bid_q.limit
        self._curr_mid = self._instrument.adjust_price(
            0.5 * (best_ask + best_bid)
        )

        # Check if the mid price is in the tick grid
        # If it is the case add/substract a half tick
        # whichever the closest to the previous mid
        half_tick = 0.5 * self.tick_size

        if is_divisible(self._curr_mid, self.tick_size):
            if self._curr_mid < self._prev_mid:
                self._curr_mid += half_tick
            else:
                self._curr_mid -= half_tick
        self._curr_mid = self._instrument.adjust_price(self._curr_mid)

    def depth(self, side) -> int:
        """Computes the depth of one side of the orderbook"""
        m = self.min_bid if side.is_bid else self.max_ask
        return side * (self.best_queue[side].limit - m) // self.tick_size

    def init_state(self, unit_size: float, bid_state: List, ask_state: List):
        """Initialize the orderbook state.
        For `bid_state` and `ask_state` each element is a tuple
        (price, volume). We assume that all orders have the same
        quantity representing for example the AES:
        Average Event Size corresponding
        to the symbol we are trading and defined by `unit_size`.

        Parameters
        ----------

        unit_size: float
            The quantity of all orders sent by `external agents`.
            Since we can know the quantities that are sent for each order,
            we assume that all quantities are constant.
            This constant is the unit_size and can be measured for example
            by taking the average of all the events of the true LOB data
            provided by the exchange

        bid_state: List
            A list of (price, volume) tuple that represents the volume present
            in each price limit.
            The number of orders is then deduced as volume // unit_size

        ask_state: List
            A list of (price, volume) tuple that represents the volume present
            in each price limit.
            The number of orders is then deduced as volume // unit_size
        """
        owner = "system"
        logger.debug("Initializing the LOB state")
        # Update the bid side of the orderbook
        for price, volume in bid_state:
            for _ in range(volume // unit_size):
                order = Order(
                    instrument=self._instrument,
                    owner=owner,
                    side=Side.BID,
                    quantity=unit_size,
                    price=price,
                )
                self.order_map[order.order_id] = order
                self._insert_order(order)

        # Update the ask side of the orderbook
        for price, volume in ask_state:
            for _ in range(volume // unit_size):
                order = Order(
                    instrument=self._instrument,
                    owner=owner,
                    side=Side.ASK,
                    quantity=unit_size,
                    price=price,
                )
                self.order_map[order.order_id] = order
                self._insert_order(order)

    def get_state(self):
        """Returns the state of the orderbook as a dictionnary
        containing the bid and ask limits and their volumes

        Returns:
        --------

        orderbook_state: dict
            The orderbook state as a dictionnary containing two keys:
            `a` for ask queues and `b` for bid queues.
            Each list contains a (price, volume) tuple.
        """
        bids = []
        asks = []
        qb = self.best_queue[Side.BID]
        qa = self.best_queue[Side.ASK]
        while exist_any(qb, qa):
            if qb is not None:
                bids.append((qb.limit, qb.volume))
                qb = qb.qnext
            if qa is not None:
                asks.append((qa.limit, qa.volume))
                qa = qa.qnext
        return dict(ts=now(), b=bids, a=asks)

    def on_limit(
        self, side: Side, quantity: float, price: float, client_id: str
    ):
        """Places a limit order on the orderbook.

        If the limit order crosses the best limits, then a marketable order is
        sent instead. See: `on_marketable` method.

        Parameters
        ---------

        side: Side
            The side of the orderbook. `Side.BID` or `Side.ASK`

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`

        price: float
            The price of the limit order at wich the buy or sell order will be
            executed.
            The price should live in the tick-grid, i.e a multiple of
            `tick_size`

        client_id: str
           The identity of the client sending the order.
        """
        best_queue = self.best_queue[-side]
        if best_queue is not None and price * side >= best_queue.limit * side:
            logger.warning(
                "Crossing the spread, sending a marketable order instead."
            )
            self.on_marketable(side, quantity, price, client_id)
        else:
            order = Order(
                instrument=self._instrument,
                owner=client_id,
                side=side,
                quantity=quantity,
                price=price,
            )
            self._insert_order(order)

    def on_marketable(
        self, side: Side, quantity: float, price: float, client_id: str
    ):
        """Places a limit order that crosses the best limits.

        Parameters
        ---------
        side: Side
            The side of the orderbook. `Side.BID` or `Side.ASK`

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`

        price: float
            The price of the limit order at wich the buy or sell order will
            be executed.
            The price should live in the tick-grid, i.e a multiple of
            `tick_size`

        client_id: str
           The identity of the client sending the order.
        """

        # walk the best limit
        q = self.best_queue[-side]
        if q is None:
            return self._reject_market(
                client_id, side=str(side), quantity=quantity, price=price
            )

        # Walk the orderbook up to the limit = price
        # while we still have liquidity to be consumed
        while quantity != 0 and side * q.limit <= side * price:
            order = q.ohead
            qty = min(quantity, order.remaining)
            q.fill(order, qty)
            self.best_volumes[order.side] = self._instrument.adjust_quantity(
                self.best_volumes[order.side] - qty
            )
            if order.filled:
                q.remove(order)
                self.order_map[order.order_id]

            quantity = self._instrument.adjust_quantity(quantity - qty)
            if q.empty:
                self._delete_queue(-side, q)
            q = self.best_queue[-side]
            if q is None:
                break

        # If there is a quantity left place a limit order
        if quantity != 0:
            order = Order(
                instrument=self._instrument,
                owner=client_id,
                side=side,
                quantity=quantity,
                price=price,
            )
            logger.warning(
                f"No more liquidity on best limits. Placing {order=!s}"
            )
            self._insert_order(order)

    def on_market(self, side: Side, quantity: float, client_id: str):
        """Executes a market order by walking the orderbook.

        Parameters
        ---------
        side: Side
            The side of the orderbook. `Side.BID` or `Side.ASK`

        quantity: float
            The order size as a multiple of `step_size`. The quantity should
            be between `min_qty` and `max_qty`

        client_id: str
           The identity of the client sending the order.
        """
        available = self.best_volumes[side]
        if quantity > available:
            self._reject_market(
                client_id=client_id,
                reason=f"{quantity=} is greater than available liquidity {available}",
                side=str(side),
                quantity=quantity
            )
            return

        remaining = quantity
        while remaining != 0:
            q = self.best_queue[side]
            if q is None:
                self._reject_market(
                    client_id, side=str(side), quantity=quantity
                )
                return

            logger.debug(
                f"Executing {side=!s}, {remaining=}. Best queue is {q!r}"
            )
            order = q.ohead
            qty = min(remaining, order.remaining)
            q.fill(order, qty)
            self.best_volumes[order.side] = self._instrument.adjust_quantity(
                self.best_volumes[order.side] - qty
            )
            logger.debug(f"Updated: {q!r}")
            if order.filled:
                q.remove(order)
                del self.order_map[order.order_id]
            logger.debug(f"Checking the {q!r}")
            if q.empty:
                self._delete_queue(side, q)
            remaining = self._instrument.adjust_quantity(remaining - qty)
        logger.info(
            f"Market order executed {client_id=}, {side=!s}, {quantity=},"
            f" {remaining=}"
        )

    def on_cancel(self, order_id: str):
        """Cancels an order

        Parameters
        ----------

        order_id: str
            The ID of the order to be canceled
        """
        order = self._get_order(order_id)
        queue = order.queue

        logger.debug(f"Order {order} is being cancelled")
        message = dict(status="Cancelled")
        message.update(order.infos())

        client_id = order.owner
        queue.remove(order)
        self.best_volumes[order.side] = self._instrument.adjust_quantity(
            self.best_volumes[order.side] - order.quantity
        )
        if queue.empty:
            self._delete_queue(order.side, queue)
        del self.order_map[order.order_id]
        self.send_private(client_id, message)

    def on_amend(self, order_id: str, quantity: float, price: float):
        """Modify an already existing order in the orderbook

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
        order = self._get_order(order_id)
        side = order.side
        logger.warning(f"Amending {order=!s} changing {quantity=}, {price=}")

        queue = order.queue
        queue.remove(order)

        self.best_volumes[order.side] = self._instrument.adjust_quantity(
            self.best_volumes[order.side] - order.quantity
        )

        if queue.empty and queue.limit != price:
            logger.info(f"Deleting {queue=!r}")
            del self.queues[queue.limit]

        # The case of marketable limit orders
        if side * price >= self.mid_price * side:
            logger.warning(
                "Modifying a limit order to a marketable limit order"
            )
            self.on_marketable(side, quantity, price, order.owner)
            return

        # We update the orders' Q if different
        if queue.limit != price:
            logger.debug(f"Finding queue with {price=}")
            if price in self.queues:
                logger.debug("Queue exists")
                new_queue = self.queues[price]
            else:
                logger.debug("Creating a new queue")
                new_queue = self._create_queue(side, price)
        else:
            new_queue = queue

        order.update(price=price, quantity=quantity)
        new_queue.add(order)
        self.best_volumes[order.side] = self._instrument.adjust_quantity(
            self.best_volumes[order.side] + quantity
        )
        logger.info(f"Amended {order=!s}")
        message = dict(status="Amended")
        message.update(order.infos())
        self.send_private(order.owner, message)

    def _create_queue(self, side: Side, price: float) -> Queue:
        if side.is_bid:
            self.min_bid = min(price, self.min_bid)
        else:
            self.max_ask = max(price, self.max_ask)

        new_queue = Queue(limit=price, side=side, notify=self.send_private)

        logger.debug(f"Creating {new_queue!r}")
        # If the side of LOB is empty
        if self.best_queue[side] is None:
            self.best_queue[side] = new_queue
            logger.debug(f"The best {side} queue is now: {new_queue!r}")
            self._update_mid()
        # If we cross the spread then we update the best limits
        elif side * price > self.best_queue[side].limit * side:
            logger.debug("Spread has been crossed, updating best limits")
            # No previous Q for a best limit Q
            new_queue.qprev = None
            # The next Q is then the current best
            new_queue.qnext = self.best_queue[side]
            # The previous Q of the current best becomes the new Q
            self.best_queue[side].qprev = new_queue
            # We are now the best limit
            self.best_queue[side] = new_queue
            self._update_mid()
            logger.debug(f"The best {side} queue is now: {new_queue!r}")

        # We are not crossing the spread then we need to find the previous
        # Q for this limit
        else:
            prev_queue = self._find_prev_queue(price, side)
            new_queue.qprev = prev_queue
            new_queue.qnext = prev_queue.qnext
            if prev_queue.qnext is not None:
                prev_queue.qnext.qprev = new_queue
            prev_queue.qnext = new_queue

        self.queues[price] = new_queue
        logger.debug(f"{new_queue!r} created.")
        logger.debug(
            f"New min bid: {self.min_bid}, new max ask = {self.max_ask}"
        )
        return new_queue

    def _find_prev_queue(self, limit: float, side: Side) -> Queue:
        it = 0
        current_limit = limit
        while True:
            it += 1
            current_limit = self._instrument.adjust_price(
                current_limit + self.tick_size * side
            )
            # logger.debug(f"testing {current_limit=}")
            current_queue = self.queues.get(current_limit)
            if current_queue is not None:
                logger.debug(
                    f"previous queue {current_queue!r} found after"
                    f" {it} iterations"
                )
                return current_queue

    def _get_order(self, order_id: str) -> Order:
        order = self.order_map.get(order_id)
        if order is None:
            e = OrderbookException(f"No order with id {order_id} found")
            logger.exception(e)
            raise e
        return order

    def _insert_order(self, order: Order):
        logger.info(f"Placing a limit {order=!s}")
        self.order_map[order.order_id] = order
        if order.price in self.queues:
            queue = self.queues[order.price]
        else:
            queue = self._create_queue(order.side, order.price)
        queue.add(order)
        self.best_volumes[order.side] = self._instrument.adjust_quantity(
            self.best_volumes[order.side] + order.quantity
        )
        logger.info(f"Placed {order=!s}")

    def _delete_queue(self, side: Side, queue: Queue):
        # Check if we are deleting a best queue
        if queue == self.best_queue[side]:
            self.best_queue[side] = queue.qnext
            if queue.qnext is not None:
                queue.qnext.qprev = None
            self._update_mid()
        else:
            # At least the best limit is a candidate to the previous queue
            queue.qprev.qnext = queue.qnext
            if queue.qnext is not None:
                queue.qnext.qprev = queue.qprev
        logger.debug(f"{queue!r} is being deleted")
        if side.is_bid and queue.limit == self.min_bid:
            if queue.qprev is None:
                self.min_bid = float("inf")
            else:
                self.min_bid = queue.qprev.limit
        if not side.is_bid and queue.limit == self.max_ask:
            if queue.qprev is None:
                self.max_ask = 0
            else:
                self.max_ask = queue.qprev.limit
        del self.queues[queue.limit]

    def _reject_market(self, client_id: str, reason=None, **kwargs):
        reason = reason or "No available liquidity in maket."
        logger.error(reason)
        message = dict(
            status="rejected",
            reason=reason,
            engine_ts=now(),
        )
        message.update(kwargs)
        self.send_private(client_id, message)

    def __str__(self):
        divider = colored("=" * 40, "blue")
        bid_str = ""
        best_bid = self.best_queue[Side.BID]
        best_ask = self.best_queue[Side.ASK]

        if best_bid is None:
            bid_str = "|\n"
        else:
            price = self.min_bid
            while price <= best_bid.limit:
                queue = self.queues.get(price)
                if queue is None:
                    bid_str += f"[V={0:<8} N={0:<3}]\tP={price:<16} |"
                else:
                    bid_str += f"[V={queue.volume:<8} N={queue.nb_orders:<3}]"
                    bid_str += f"\tP={price:<16} |{queue}"
                bid_str += "\n"
                price = self._instrument.adjust_price(price + self.tick_size)

        ask_str = ""
        if best_ask is None:
            ask_str = "|"
        else:
            price = best_ask.limit
            while price <= self.max_ask:
                queue = self.queues.get(price)
                if queue is None:
                    ask_str += f"[V={0:<8} N={0:<3}]\tP={price:<16} |"
                else:
                    ask_str += f"[V={queue.volume:<8} N={queue.nb_orders:<3}]"
                    ask_str += f"\tP={price:<16} |{queue}"
                ask_str += "\n"
                price = self._instrument.adjust_price(price + self.tick_size)

        mid = self.mid_price
        if mid is not None:
            spread_string = " " * 19 + f"\tP={mid:<16} |{divider}"
        else:
            spread_string = f"{divider}"
        header = f"Orderbook for symbol {self._instrument.symbol}:\n"
        header += f"Total bid volume {self.best_volumes[Side.BID]}\t"
        header += f"Total ask volume {self.best_volumes[Side.ASK]}\n\n"
        return f"{header}{bid_str}{spread_string} Mid-price\n{ask_str}"
