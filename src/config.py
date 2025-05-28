from dataclasses import dataclass


@dataclass
class NetworkConfig:
    host: str = "localhost"
    port: int = 9876


@dataclass
class OrderbookConfig:
    symbol: str
    tick_size: float
    min_qty: float
    max_qty: float
    step_size: float
    price_precision: int
    volume_precision: int


@dataclass
class ExchangeConfig:
    trades_freq: float = 0.01
    quotes_freq: float = 0.01
