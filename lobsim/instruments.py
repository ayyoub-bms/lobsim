from dataclasses import dataclass
from lobsim.utils import is_divisible


@dataclass
class LotSize:
    max_qty: float
    min_qty: float
    step_size: float


@dataclass
class PriceDetails:
    tick_size: float
    min_price: float
    max_price: float


@dataclass
class Precision:
    price_precision: int
    quote_precision: int
    quantity_precision: int
    base_asset_precision: int


@dataclass
class MarginDetails:
    margin_pct: float
    m_margin_pct: float
    margin_asset: str


@dataclass
class Fees:
    liquidation_fee: float
    taking_fee: float


@dataclass
class Instrument:
    symbol: str
    contract_type: str
    base_asset: str
    quote_asset: str
    trigger_protect: float
    fees: Fees
    lot_size: LotSize
    precision: Precision
    price_details: PriceDetails
    margin_details: MarginDetails

    def is_valid_quantity(self, quantity: float) -> bool:
        lot_size = self.lot_size
        if quantity > lot_size.max_qty:
            return False
        if quantity < lot_size.min_qty:
            return False
        if not is_divisible(quantity, lot_size.step_size):
            return False

        return True

    def is_valid_price(self, price: float) -> bool:
        pd = self.price_details
        if price < pd.min_price:
            return False
        if price > pd.max_price:
            return False
        if not is_divisible(price, pd.tick_size):
            return False

        return True

    def adjust_price(self, price):
        return round(price, self.precision.price_precision)

    def adjust_quantity(self, quantity):
        return round(quantity, self.precision.quantity_precision)


test_instrument = Instrument(
    symbol="TEST SYMBOL",
    contract_type="TEST CONTRACT TYPE",
    base_asset="TEST BASE",
    quote_asset="TEST QUOTE",
    trigger_protect=0.1,
    fees=None,
    lot_size=LotSize(max_qty=100, min_qty=1, step_size=1),
    precision=Precision(
        price_precision=2,
        quote_precision=2,
        quantity_precision=0,
        base_asset_precision=0,
    ),
    price_details=PriceDetails(tick_size=0.1, min_price=0.1, max_price=10000),
    margin_details=None,
)

