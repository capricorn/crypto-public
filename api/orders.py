# Orders perform atomic operations on the server, and then wait for feedback
# via order events. They typically operate with an internal state.
# I don't want IOC; Instead, kill the order after a partial fill

# Some order types require both order events /and/ the orderbook; others do not
# Some order types are merely concerned with the execution status of an order, and
# whether it succeeded or not.

# Since orders need to have the autonomy to place and cancel orders, they'll need to be initialized
# with an api reference.
# Wondering if there will be an issue when we need to algorithmically place orders on two different exchanges..

# Need to also explore if each exchange at a minimum produces an Open order. I think CB only does when an order
# is placed on the books, not if an immediate cancellation or fill occurs.

# Some order updates will depend on the state of the OB alongside current order state. Recommended simply to
# init with a reference to the OB

from decimal import Decimal

class PegBestOrder():
    '''
    This type of order pegs the order at the best price on the desired orderbook side.
    It automatically cancels and replaces orders when necessary, and also handles partial fills
    on cancellation, etc.

    The order finishes when filled.

    PegBestOrder can be thought of as a composition of the following orders:
    - OCO(CancelIfTouchedOrder, OSO(CancelRemainderOrder, IOC))

    CancelIfTouchedOrder occurs if the best market price is better than ours. If CancelRemainderOrder executes,
    then an IOC order is submitted that performs our taker operation. Wrapping IOC in a MarketOrder that liquidates
    any leftovers would be best.

    If one of these executes, the other is cancelled. PegBestOrder is unique in that is a self-adjusting
    order, dependent on the outcome of OCO. 
    '''
    def __init__(self, order_id):
        self.order_id = order_id
        self.done = False

    def update(self, order_event):
        if order_event.event_type == 'order_open':
            pass
        elif order_event.event_type == 'order_match':
            pass
        elif order_event.event_type == 'order_done':
            pass

class OCO():
    pass

class OSO():
    pass

class RedoUntilOrder():
    '''
    Type of order that continually resubmits an order (regardless of cancellation or execution) until cancelled
    '''
    pass

class CancelIfTouchedOrder():
    '''
    Cancel the given order if the best market price reaches some price p.

    - We always get an order open event
    - We always get an order done event

    The init process we place some limit order, and obtain its order id.
    Websockets will generate the order_open event, which is where we begin tracking

    We should get notified from websocket events if an order failed from post only, etc.
    But will it have an order id..?
    '''
    def __init__(self, order_id):
        pass
    
    def update(self, order_event):
        pass

class CancelRemainderOrder():
    '''
    This order type is an order that cancels the remaining transaction on the book when
    any size of fill occurs. If the entire order is filled, no cancellation occurs.
    '''
    pass

# What useful information would we like to know after an IOCOrder is done..?
# Perhaps it would be most useful to know the remaining quantity (or the executed quantity..?)
# Could probably just reduce to LimitOrder
class IOCOrder():
    '''
    PoC

    Place an IOC limit order.
    '''

    def __init__(self, api, pair, side, price, quantity):
        self.api = api
        self.pair = pair
        self.side = side
        self.price = Decimal(price)
        self.quantity = Decimal(quantity)
        self.done = False

    def execute(self):
        if self.side == 'ask':
            self.api.limit_sell_order(self.convert_pair(self.pair), self.price, self.quantity, IOC=True)
        elif self.side == 'bid':
            self.api.limit_buy_order(self.convert_pair(self.pair), self.price, self.quantity, IOC=True)
        else:
            raise ValueError(f'Unrecognized order side: {self.side}')

    def update(self, update_event):
        if update_event.event_type == 'order_open':
            pass
        elif update_event.event_type == 'order_match':
            pass
        elif update_event.event_type == 'order_done':
            self.done = True
            if update_event.reason == 'cancelled':
                pass
            elif update_event.reason == 'filled':
                pass
