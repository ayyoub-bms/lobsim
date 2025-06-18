import json
import asyncio
from decimal import Decimal
from datetime import datetime
from typing import TypeAlias

Timestamp: TypeAlias = float


class _PubSub:
    """Publish-subscribe way to broadcast messages
    Taken from:
        https://websockets.readthedocs.io/en/stable/topics/broadcast.html
    """

    def __init__(self):
        self.waiter = asyncio.get_running_loop().create_future()

    def publish(self, value):
        waiter = self.waiter
        self.waiter = asyncio.get_running_loop().create_future()
        waiter.set_result((value, self.waiter))

    async def __aiter__(self):
        """This is the part where the subscription takes palce"""
        waiter = self.waiter
        while True:
            value, waiter = await waiter
            yield value


def now():
    return datetime.now().timestamp()


def exist_none(*args):
    return all(item is None for item in args)


def exist_any(*args):
    return not exist_none(*args)


def exist_all(*args):
    return all(item is not None for item in args)


def is_divisible(x: float, y: float) -> bool:
    return y != 0 and Decimal(str(x)) % Decimal(str(y)) == 0


def build_message(event, **kwargs):
    msg_dict = dict(event=event)
    msg_dict.update(kwargs)
    return json.dumps(msg_dict)
