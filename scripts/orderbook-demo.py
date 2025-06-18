import sys
import uuid
import numpy as np
import time

from lobsim.orderbook import Orderbook
from lobsim.orders import Side
from lobsim.utils import is_divisible
from lobsim.instruments import test_instrument

order_ids = []


def private_message(client_id, message):
    status = message['status']
    if status == 'New order':
        order_ids.append(message['order_id'])
    elif status in ('Filled', 'Cancelled'):
        order_ids.remove(message['order_id'])


client_id = str(uuid.uuid4())
lob = Orderbook(test_instrument, send_private=private_message)


def gen_limit():
    inf = float('inf')
    max_bid = 0
    min_ask = inf
    prev_mid = None
    curr_mid = None
    tick_size = test_instrument.price_details.tick_size
    while True:
        prev_mid = curr_mid
        curr_mid = round(.5 * (max_bid + min_ask), 2)
        if curr_mid == inf:
            curr_mid = 100.05

        if is_divisible(curr_mid, tick_size):
            if prev_mid is not None and curr_mid < prev_mid:
                curr_mid = round(curr_mid + tick_size / 2, 2)
            else:
                curr_mid = round(curr_mid - tick_size / 2, 2)
        price = round(curr_mid + (1 - 2 * np.random.rand()), 1)
        if price < curr_mid:
            side = Side.BID
        else:
            side = Side.ASK
        qty = np.random.randint(10, 50)

        if side.is_bid:
            max_bid = max(price, max_bid)
        else:
            min_ask = min(price, min_ask)
        yield (side, price, qty)


def gen_cancel():
    return order_ids[np.random.randint(0, len(order_ids))]


order_details = None


def order():
    global order_details
    while True:
        cancel, limit, market = np.random.multinomial(1, pvals=[.3, .5, .2])
        if limit:
            side, price, quantity = next(gen_limit())
            lob.on_limit(side=side, price=price, quantity=quantity, client_id=client_id)
            order_details = f'Limit Order({side=!s}, {price=}, {quantity=})'
            yield order_details
        if cancel:
            if len(order_ids) == 0:
                continue
            order_id = gen_cancel()
            order = lob.order_map.get(order_id)
            side = order.side
            quantity = order.quantity
            price = order.price
            lob.on_cancel(order_id)
            order_details = f'Cancel Order({side=}, {price=}, {quantity=})'
            yield order_details
        if market:
            quantity = np.random.randint(1, 30)
            side = Side.ASK if np.random.randint(2) else Side.BID
            if lob.best_queue[side] is None or lob.best_queue[side].empty:
                continue
            lob.on_market(side=side, quantity=quantity, client_id=client_id)
            order_details = f'Market Order({side=}, {quantity=}'
            yield order_details


if __name__ == '__main__':
    while True:
        order_details = next(order())
        if order_details is not None:
            string = f'Previous event: {order_details})\n\n'
        string += str(lob)
        print(string, end="\r", sep="")
        sys.stdout.write("\033[2J\033[H")
        time.sleep(.05)
