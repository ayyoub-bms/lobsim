import pytest
from lobsim.orderbook import Orderbook
from lobsim.orders import Side
from lobsim.instruments import Instrument, LotSize, Precision, PriceDetails


def send_private(client_id=None, message=None):
    print(client_id, message)


@pytest.fixture
def instrument():
    return Instrument(
        symbol="TEST SYMBOL",
        contract_type="TEST CONTRACT TYPE",
        base_asset="TEST BASE",
        quote_asset="TEST QUOTE",
        trigger_protect=0.1,
        fees=None,
        lot_size=LotSize(max_qty=100, min_qty=1, step_size=1),
        precision=Precision(
            price_precision=4,
            quote_precision=3,
            quantity_precision=5,
            base_asset_precision=0,
        ),
        price_details=PriceDetails(
            tick_size=0.001, min_price=0.1, max_price=10000
        ),
        margin_details=None,
    )


@pytest.fixture
def tick_size(instrument):
    return instrument.price_details.tick_size


@pytest.mark.parametrize(
    "input_list, expected",
    (
        ([], "None"),  # Empty Q
        ([[Side.ASK, 1, 1]], "1 - 0.5 * tick_size"),  # Empty bid
        ([[Side.BID, 1, 1]], "1 + 0.5 * tick_size"),  # Empty ask
        (
            [[Side.ASK, 1, 1]],
            "1 - 0.5 * tick_size",
        ),  # Empty ask
        (
            [
                [Side.ASK, 1, 2.2],
                [Side.BID, 1, 1],
            ],
            "1.6005",
        ),  # In grig ASK first
        (
            [
                [Side.BID, 1, 1],
                [Side.ASK, 1, 2.2],
            ],
            "1.5995",
        ),  # In grid BID first
        (
            [
                [Side.BID, 1, 2.001],
                [Side.ASK, 1, 2.002],
            ],
            "2.0015",
        ),  # Not in grid tick
    ),
)
def test_mid_price(tick_size, input_list, expected, instrument):
    lob = Orderbook(instrument=instrument, send_private=send_private)
    for lst in input_list:
        side, quantity, price = lst
        lob.on_limit(
            side=side, quantity=quantity, price=price, client_id="test_mid"
        )
    assert lob.mid_price == eval(expected)


# TODO: ensure rejection and order events are called
class TestOnLimit:
    # TODO: Further refactoring necessary no need for all this
    @pytest.mark.parametrize("side", (Side.ASK, Side.BID))
    def test_single_side_inserts(self, instrument, side):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        price = 2.002
        other = 2.002 - side * 1
        for i in range(1, 3):
            lob.on_limit(side, quantity=1, price=price, client_id="test_mid")
            side_q = lob.best_queue[side]
            opposite_q = lob.best_queue[-side]
            assert opposite_q is None
            assert side_q.limit == price
            assert side_q.nb_orders == i
            assert side_q.volume == i
            assert lob.best_volumes[side] == i
            assert lob.best_volumes[-side] == 0

        # Order 3: insert far from best
        lob.on_limit(side, quantity=1, price=other, client_id="test_mid")
        assert side_q.qprev is None
        assert side_q.qnext.limit == other
        assert side_q.qnext.volume == 1
        assert lob.best_volumes[side] == 3
        assert lob.best_volumes[-side] == 0

    @pytest.mark.parametrize(
        "side, s_price, o_price",
        ((Side.ASK, 3.002, 2), (Side.BID, 1.002, 2.002)),
    )
    def test_opposite(self, side, s_price, o_price, instrument):
        # Add to opposite queue
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=1, price=s_price, client_id="test_mid")
        lob.on_limit(-side, quantity=1, price=o_price, client_id="test_mid")
        opposite_q = lob.best_queue[-side]
        assert not opposite_q.empty
        assert opposite_q.nb_orders == 1
        assert opposite_q.volume == 1

    @pytest.mark.parametrize(
        "side, quantity1, quantity2, price1, price2, is_partial",
        [
            # Full exectution
            [Side.BID, 5, 1, 2.002, 1.002, False],
            [Side.ASK, 5, 1, 1.002, 2.002, False],
            # Partial Execution
            [Side.BID, 1, 5, 2.002, 1.002, True],
            [Side.ASK, 1, 5, 1.002, 2.002, True],
        ],
    )
    def test_marketable(
        self,
        side,
        quantity1,
        quantity2,
        price1,
        price2,
        is_partial,
        instrument,
    ):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(
            side, quantity=quantity1, price=price1, client_id="test_mid"
        )
        lob.on_limit(
            -side, quantity=quantity2, price=price2, client_id="test_mid"
        )
        opposite_q = lob.best_queue[-side]
        side_q = lob.best_queue[side]
        if is_partial:
            assert side_q is None
            assert not opposite_q.empty
            assert opposite_q.nb_orders == 1
            assert opposite_q.volume == abs(quantity1 - quantity2)
        else:
            assert opposite_q is None
            assert not side_q.empty
            assert side_q.nb_orders == 1
            assert side_q.volume == abs(quantity1 - quantity2)

    @pytest.mark.parametrize(
        "side, quantity, prices",
        [
            [Side.BID, 12, [2.002, 2.001, 2.000]],
            [Side.ASK, 12, [2.000, 2.001, 2.002]],
        ],
    )
    def test_deep_marketable(self, side, quantity, prices, instrument):
        n = len(prices)
        lob = Orderbook(instrument=instrument, send_private=send_private)
        for i in range(n):
            lob.on_limit(
                side, quantity=1, price=prices[i], client_id="test_mid"
            )
        lob.on_limit(
            -side, quantity=quantity, price=prices[-1], client_id="test_mid"
        )
        opposite_q = lob.best_queue[-side]
        side_q = lob.best_queue[side]
        assert side_q is None
        assert not opposite_q.empty
        assert opposite_q.nb_orders == 1
        assert opposite_q.volume == quantity - n

    @pytest.mark.parametrize(
        "side, prices",
        [
            [Side.BID, [2.000, 3.002, 3.000]],
            [Side.ASK, [3.002, 2.000, 2.500]],
        ],
    )
    def test_inside_lob(self, side, prices, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        n = len(prices)
        lob.on_limit(side, quantity=1, price=prices[0], client_id="test_mid")
        for i in range(1, n):
            lob.on_limit(
                -side, quantity=1, price=prices[i], client_id="test_mid"
            )

        side_q = lob.best_queue[-side]
        assert side_q.limit == prices[-1]
        assert side_q.qnext.limit == prices[-2]

    @pytest.mark.parametrize("side", (Side.BID, Side.ASK))
    def test_outside_lob(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        price = 5
        lob.on_limit(side, quantity=1, price=price, client_id="test_mid")
        lob.on_limit(
            -side, quantity=1, price=price + .01 * side, client_id="test_mid"
        )
        lob.on_limit(
            side, quantity=1, price=price - .02 * side, client_id="test_mid"
        )
        best_q = lob.best_queue[side]
        assert best_q.qnext.limit == price - .02 * side

    @pytest.mark.parametrize("side", (Side.BID, Side.ASK))
    def test_cross_bid_ask(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        price = 5
        lob.on_limit(side, quantity=1, price=price, client_id="test_mid")
        lob.on_limit(side, quantity=1, price=price - .01 * side, client_id="test_mid")
        lob.on_limit(
            -side, quantity=1, price=price - .02 * side, client_id="test_mid"
        )
        # One queue depleted
        assert lob.best_queue[side].limit == price - .01 * side

    @pytest.mark.parametrize(
        "side, quantity, s_price, o_price, l_price, n",
        [
            [Side.ASK, 4, 1.8, 1.3, 1.4, 3],
            [Side.BID, 4, 1.3, 1.8, 1.7, 3],
        ],
    )
    def test_new_other_best(
        self, side, quantity, s_price, o_price, l_price, n, instrument
    ):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        for i in range(n):
            lob.on_limit(side, quantity=1, price=s_price, client_id="test_mid")
            lob.on_limit(
                -side, quantity=1, price=o_price, client_id="test_mid"
            )
        lob.on_limit(
            side, quantity=quantity, price=l_price, client_id="test_mid"
        )

        opposite_q = lob.best_queue[-side]
        side_q = lob.best_queue[side]
        assert side_q.limit == l_price
        assert side_q.nb_orders == 1
        assert side_q.volume == quantity
        assert opposite_q.limit == o_price
        assert opposite_q.volume == quantity - 1


@pytest.mark.parametrize("side", (Side.ASK, Side.BID))
class TestOnCancel:

    def test_when_single_order(self, instrument, side):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=1, price=3.002, client_id="test_mid")
        order_id = list(lob.order_map.keys())[0]
        order = lob.order_map.get(order_id)
        assert order.onext is None
        assert order.oprev is None
        assert order == lob.best_queue[side].ohead
        assert order == lob.best_queue[side].otail
        lob.on_cancel(order_id)
        assert lob.best_queue[side] is None
        self.last_cancelled = order

    def test_when_middle_order(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        for i in range(1, 4):
            lob.on_limit(side, quantity=i, price=3.002, client_id="test_mid")

        # Cancel middle
        order_id = next(
            o.order_id for o in lob.order_map.values() if o.quantity == 2
        )
        order = lob.order_map.get(order_id)
        assert order.onext.quantity == 3
        assert order.oprev.quantity == 1
        lob.on_cancel(order_id)
        assert order_id not in lob.order_map
        assert lob.best_queue[side].volume == 4
        assert lob.best_queue[side].nb_orders == 2
        self.last_cancelled = order

    def test_when_head_order(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=1, price=3.002, client_id="test_mid")
        lob.on_limit(side, quantity=2, price=3.002, client_id="test_mid")
        head_id = next(
            o.order_id for o in lob.order_map.values() if o.quantity == 1
        )
        # Cancel Head
        order = lob.order_map[head_id]
        tail_id = next(
            o.order_id for o in lob.order_map.values() if o.quantity == 2
        )
        tail = lob.order_map[tail_id]
        assert order.oprev is None
        assert order.onext == tail
        lob.on_cancel(head_id)
        assert head_id not in lob.order_map
        assert lob.best_queue[side].volume == 2
        assert lob.best_queue[side].nb_orders == 1

    def test_when_tail_order(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=1, price=3.002, client_id="test_mid")
        lob.on_limit(side, quantity=2, price=3.002, client_id="test_mid")
        head_id = next(
            o.order_id for o in lob.order_map.values() if o.quantity == 1
        )
        order = lob.order_map[head_id]
        tail_id = next(
            o.order_id for o in lob.order_map.values() if o.quantity == 2
        )
        tail = lob.order_map[tail_id]
        assert tail.oprev == order
        assert tail.onext is None
        lob.on_cancel(tail_id)
        assert order.onext is None
        assert order.oprev is None
        assert tail_id not in lob.order_map
        assert lob.best_queue[side].volume == 1
        assert lob.best_queue[side].nb_orders == 1


@pytest.mark.parametrize("side", (Side.BID, Side.ASK))
class TestOnMarket:

    def test_consume_all_liquidity_1Q(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=10, price=3.002, client_id="test_mid")
        lob.on_market(side, quantity=10, client_id="test_mid")
        assert lob.best_queue[side] is None
        assert lob.best_queue[-side] is None

    def test_consume_portion_of_liquidity_1Q(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=10, price=3.002, client_id="test_mid")
        lob.on_market(side, quantity=5, client_id="test_mid")
        assert lob.best_queue[side].volume == 5

    def test_consume_portfion_of_liquidity_nQ(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        tick = side * 0.001
        price = instrument.adjust_price(3.002 - tick)
        lob.on_limit(side=side, quantity=10, price=3.002, client_id="test_mid")
        lob.on_limit(
            side=side,
            quantity=20,
            price=price,
            client_id="test_mid",
        )
        lob.on_market(side, quantity=15, client_id="test_mid")
        assert lob.best_queue[side].nb_orders == 1
        assert lob.best_queue[side].limit == price
        assert lob.best_queue[side].volume == 15

    def test_consume_all_liquidity_nQ(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=10, price=3.003, client_id="test_mid")
        tick = side * 0.001
        price = instrument.adjust_price(3.002 - tick)
        lob.on_limit(
            side=side,
            quantity=20,
            price=price,
            client_id="test_mid",
        )
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_market(side, quantity=30, client_id="test_mid")
        assert lob.best_queue[side] is None

    def test_exceed_available_liquidity(self, side, instrument):
        lob = Orderbook(instrument=instrument, send_private=send_private)
        lob.on_limit(side, quantity=10, price=3, client_id="test_mid")
        lob.on_market(side, quantity=15, client_id="test_mid")
        assert lob.best_queue[side].volume == 10
        # We are supposed to get a rejection message for the remaining quantity


class TestOnAmend:

    @pytest.fixture(autouse=True)
    def init(self, instrument):
        self.lob = Orderbook(instrument=instrument, send_private=send_private)
        bid_data = [[5, 3.1], [10, 3.1], [15, 3.1], [4, 3], [6, 3]]
        ask_data = [[11, 3.5], [9, 3.5], [8, 3.9], [12, 3.9], [20, 3.9]]
        for i in range(5):
            self.lob.on_limit(Side.BID, *bid_data[i], client_id="test_mid")
            self.lob.on_limit(Side.ASK, *ask_data[i], client_id="test_mid")

    def amend_quantity(self):
        order_id = next(
            o.order_id for o in self.lob.order_map.values() if o.quantity == 8
        )
        order = self.lob.order_map.get(order_id)
        self.lob.on_amend(order_id, quantity=3, price=order.price)
        assert order.queue.volume == 35
        q = order.queue
        self.lob.on_amend(order_id, quantity=3, price=3.5)
        assert q.volume == 32
        assert q.nb_orders == 2
        assert order.queue.nb_orders == 3
        assert order.queue.limit == 3.5
        assert order.queue.volume == 23

    def amend_quantity_and_limit(self):
        order_id = next(
            o.order_id for o in self.lob.order_map.values() if o.quantity == 20
        )
        order = self.lob.order_map.get(order_id)
        q = order.queue
        self.lob.on_amend(order_id, quantity=5, price=3.5)
        assert q.volume == 12
        assert q.nb_orders == 1
        assert order.queue.limit == 3.5
        assert order.queue.volume == 28
        assert order.queue.nb_orders == 4

    def amend_to_marketable(self):
        order_id = next(
            o.order_id for o in self.lob.order_map.values() if o.quantity == 12
        )
        order = self.lob.order_map.get(order_id)
        q = order.queue
        self.lob.on_amend(order_id, quantity=12, price=2)
        assert self.lob.best_queue[Side.BID].volume == 18
        assert 3.9 not in self.lob.queues
        assert q.nb_orders == 0
        assert q.empty
