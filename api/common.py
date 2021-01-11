from collections import namedtuple
from decimal import Decimal

Fees = namedtuple('Fees', 'maker_fee taker_fee')
Wallet = namedtuple('Wallet', 'asset balance')
CurrencyInfo = namedtuple('CurrencyInfo', 'name id min_size min_withdrawal max_precision')
BookEntry = namedtuple('BookEntry', 'price quantity')

class ExchangeException(Exception):
    def __init__(self, exchange, reason):
        self.exchange = exchange
        self.reason = reason

class Event():
    def __init__(self, event_type, exchange):
        self.event_type = event_type
        self.exchange = exchange

    def __eq__(self, other):
        return isinstance(other, Event) and self.event_type == other.event_type

class SubscriptionsEvent(Event):
    def __init__(self, exchange, msg=None):
        super().__init__('subscriptions', exchange)
        self.msg = msg

class HeartBeatEvent(Event):
    def __init__(self, exchange):
        super().__init__('heartbeat', exchange)

class MatchEvent(Event):
    def __init__(self, exchange, price, volume, side, ordertype=None, maker_id=None, taker_id=None):
        super().__init__('match', exchange)
        self.price = price
        self.volume = volume
        self.side = side
        self.ordertype = ordertype
        self.maker_id = maker_id
        self.taker_id = taker_id

class OrderbookSnapshotEvent(Event):
    def __init__(self, exchange, snapshot):
        super().__init__('orderbook_snapshot', exchange)
        self.snapshot = snapshot

class OrderbookEvent(Event):
    def __init__(self, exchange, update, sequence=None, timestamp=None):
        super().__init__('orderbook_update', exchange)
        self.update = update
        self.sequence = sequence
        self.timestamp = timestamp

class OrderEvent(Event):
    def __init__(self, exchange, leq=0, lep=0, **order):
        super().__init__('order', exchange)
        self.id = order['id']
        self.quantity = order['quantity']
        self.price = order['price']
        self.status = order['status']
        self.leq = leq
        self.lep = lep

class OrderReceivedEvent(Event):
    def __init__(self, exchange, order_id, price, quantity=None, order_type=None, timestamp=None, side=None):
        super().__init__('order_received', exchange)
        self.id = str(order_id)
        self.order_type = order_type
        # Initial desired order quantity
        self.quantity = Decimal(quantity)
        self.price = Decimal(price)
        self.side = side
        self.timestamp = timestamp

class OrderMatchEvent(Event):
    def __init__(self, exchange, order_id, price, quantity, side=None, sequence=None, timestamp=None):
        super().__init__('order_match', exchange)
        self.id = str(order_id)
        self.price = Decimal(price)
        self.side = side
        # Quantity traded
        self.quantity = Decimal(quantity)
        self.sequence = sequence

class OrderOpenEvent(Event):
    def __init__(self, exchange, order_id, price, quantity, side=None, sequence=None, timestamp=None):
        super().__init__('order_open', exchange)
        self.id = str(order_id)
        self.price = Decimal(price)
        # Initial size on the books. May differ from received quantity if partially filled
        self.quantity = Decimal(quantity)
        self.side = side
        self.sequence = sequence
        self.timestamp = timestamp

class OrderDoneEvent(Event):
    def __init__(self, exchange, order_id, reason, side=None, price=None, quantity=None, timestamp=None, remaining_size=None):
        super().__init__('order_done', exchange)
        self.id = str(order_id)
        self.reason = reason
        self.side = side
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp
        self.remaining_size = remaining_size

class OrderException(Exception):
    def __init__(self, errors):
        self.errors = errors

class Order():
    STATE_OPEN = 0
    STATE_PARTIAL_FILL = 1
    STATE_FILLED = 2
    STATE_DONE = 3

    def __init__(self, order_id, ordertype, side, size, price=None):
        self.order_id = str(order_id)
        self.ordertype = ordertype
        self.side = side
        self.size = Decimal(size)
        self.price = Decimal(price)

    def __str__(self):
        return f'ID: {self.order_id}\n' +\
                f'Type: {self.ordertype}\n' +\
                f'Side: {self.side}\n' +\
                f'Size: {self.size}\n' +\
                f'Price: {self.price}\n'

    def __eq__(self, other_order):
        return isinstance(other_order, Order) and other_order.order_id == self.order_id

class ProductInfo():
    def __init__(self, pair, base_currency, quote_currency, base_precision, quote_precision, min_notional):
        self.pair = pair
        self.base_currency = base_currency
        self.quote_currency = quote_currency
        self.base_precision = base_precision
        self.quote_precision = quote_precision
        self.min_notional = min_notional
