import asyncio
import logging
from math import ceil, floor
from decimal import Decimal as D
from collections import namedtuple
from itertools import groupby

from api import trade, common, BinanceAPI, CoinbaseAPI
from util import util

#Balance = namedtuple('Balance', 'inventory quote')
class Balance():
    def __init__(self, inventory, quote):
        self.inventory = inventory
        self.quote = quote

'''
Steps

- Get exchange info
- Get current asset balance for both currencies (ex: BTC/USD = balance(BTC), balance(USD))
- Examine current OB state
- Check if spread is sufficient, place bid and ask orders (differentiate bid and ask orders..? Identify current orders)
- Track the status of these orders
- Cancel and replace if spread contracts and is still profitable
- On fills, adjust order on other side of the book
- Repeat
'''

class MakerOrder():
    def __init__(self, status, price, quantity):
        self.status = status
        self.price = price
        self.quantity = quantity

'''
In the case of bid bot (for USDT/USD), the inventory is USDT, and the quote is USD. That is, we acquire USDT using USD. Orders depend on our USD balance.
For the ask bot, we acquire USD using USDT. Orders depend on our USDT balance. 
'''
class MakerBot():
    ActiveOrder = namedtuple('ActiveOrder', 'order should_cancel')

    def __init__(self, exchange_api, balance, pair, side, min_spread, unit_size):
        '''
        min_spread is denoted in ticks
        unit_size is an integer number of base items to purchase, if possible
        '''

        # Obtain all of these via api calls
        self.side = side
        self.exchange_api = exchange_api
        self.pair = pair
        pair_info = list(filter(lambda p: p.pair == exchange_api.convert_pair(self.pair), exchange_api.get_products()))[0]
        self.active_order = None

        self.quote_precision = pair_info.quote_precision
        self.base_precision = pair_info.base_precision

        self.min_spread = self.quote_precision * D(min_spread)
        self.unit_size = D(unit_size)

        # Same reference to balance will cause issues.. Need to think about this
        # Just need to correctly withold funds when orders are placed, and then correctly reduce as orders are matched or cancelled.
        # Perhaps we'll need a local variable of sorts for tracking some delta?
        self.balance = balance
        self.active_funds = 0
        self.weight = 0

        fees = self.exchange_api.get_fees()
        self.maker_fee = fees.maker_fee
        self.taker_fee = fees.taker_fee

        # Mapping of order id to tuple containing order, and cancellation truth function
        self.orders = {}

        self.log = logging.getLogger(side)
        self.log.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        #handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(f'%(asctime)s %(name)s %(levelname)s {self.side} %(message)s'))
        self.log.addHandler(handler)
        
        self.notional_size = D(15)
        self.notional_transacted = 0

        self.STATE_RUN = 0
        self.STATE_CANCELLING = 1

        self.state = self.STATE_RUN

        self.log.debug(f'Quote precision: {self.quote_precision} Base precision: {self.base_precision}')

    def cancel(self, order_id):
        raise NotImplementedError

    def order(self, price, quantity):
        raise NotImplementedError

    def adjust_quote_by_min(self, price):
        raise NotImplementedError

    def better_price(self, p1, p2):
        raise NotImplementedError

    def order_quantity(self, price):
        raise NotImplementedError

    def handle_match_event(self, event):
        raise NotImplementedError

    def handle_done_event(self, event):
        raise NotImplementedError

    def adjust_order_balance(self, amount):
        raise NotImplementedError

    def adjust_match_balance(self, amount):
        raise NotImplementedError

    def purchase_power(self, best_price):
        raise NotImplementedError

    def handle_bbo(self, best_entry, opp_entry, spread):
        if self.state == self.STATE_RUN:
            best_price, best_quantity = best_entry
            best_opp_price, best_opp_quantity = opp_entry

            # Leave the order on the books if the spread is too small
            # Need to cancel order if spread shrinks between other side of the book (ex: ask - our bid price tells us the order's spread)
            better_order_option = self.active_order and (self.better_price(best_price, self.active_order.price) and spread >= self.min_spread)
            # We need to know the spread on the opposite side of the book, not the best_price on our side of the book!
            spread_shrank = self.active_order and abs(best_opp_price - self.active_order.price) < self.min_spread
            if self.active_order:
                self.log.debug(f'Current bid spread: {best_opp_price - self.active_order.price}')

            #if self.active_order and (better_order_option or spread_shrank):
            if better_order_option or spread_shrank:
                self.log.debug(f'better_order_option: {better_order_option} spread_shrank: {spread_shrank}')
                try:
                    self.cancel(self.active_order.order_id)
                    self.log.info(f'Cancelled order {self.active_order.order_id}')
                except common.ExchangeException as e:
                    self.log.warning(f'Failed to cancel order {self.active_order.order_id}. Reason: {e.reason}')

                self.state = self.STATE_CANCELLING
                return

            if not self.active_order:
                try:
                    new_price = best_price
                    if spread > self.min_spread:
                        new_price = self.increase_quote_by_min(new_price)
                    elif spread < self.min_spread:
                        new_price = self.decrease_quote_by_min(new_price, ticks=(self.min_spread-spread).quantize(self.quote_precision) * (D(1) / self.quote_precision))
                    self.log.debug(f'Spread: {spread} New price: {new_price} old price: {best_price}')

                    self.log.info(f'Placing order {new_price}@{self.order_quantity(new_price)}')
                    if self.purchase_power(new_price) > self.notional_size:
                        order = self.order(new_price, self.order_quantity(new_price))
                        self.log.info(f'Placed order {order.order_id} ({order.price}@{order.size})')
                        spread -= abs(new_price - best_price)
                        self.active_order = order
                        self.active_order.filled = D(0)
                    else:
                        self.log.info(f'Cannot place order. Insufficient funds: {self.purchase_power(best_price)}')
                except common.ExchangeException as e:
                    self.log.warning(f'Failed to place order. Reason: {e.reason}')

        elif self.state == self.STATE_CANCELLING:
            self.log.debug(f'Waiting for order {self.active_order.order_id} to finish')

        return spread

class BidBot(MakerBot):
    SIDE = 'BID'

    def __init__(self, exchange_api, balance, pair, min_spread, unit_size):
        super().__init__(exchange_api, balance, pair, self.SIDE, min_spread, unit_size)

    def order(self, price, quantity):
        try:
            order = self.exchange_api.limit_buy_order(self.pair, price, quantity, post_only=True)
            #order.filled = 0
            self.balance.quote -= (price * quantity).quantize(self.quote_precision)
            #self.active_funds += (price * quantity).quantize(self.quote_precision)
            return order
        except common.ExchangeException as e:
            raise e

    def increase_quote_by_min(self, price, ticks=1):
        '''
        Adjust the quote by the minimum allowed quantity, in a competitive direction
        '''
        return price + self.quote_precision * ticks

    def decrease_quote_by_min(self, price, ticks=1):
        '''
        Adjust the quote by the tick size, in the 'worse' direction
        '''
        return price - self.quote_precision * ticks

    def better_price(self, p1, p2):
        '''
        Return true if p1 is a better bid than p2
        '''
        return p1 > p2

    def order_quantity(self, price):
        '''
        Return an order that, at smallest, is the minimum notional size. 
        '''
        #min_quantity = (self.notional_size / price).quantize(self.base_precision)
        #return max(min_quantity, self.unit_size)
        return self.unit_size

    # Order is an ActiveOrder tuple
    def cancel(self, order_id):
        #order = order.order
        try:
            self.exchange_api.cancel_order(order_id)
        except common.ExchangeException as e:
            raise e

    def handle_match_event(self, event):
        self.balance.inventory += event.quantity
        self.active_order.filled += (event.price * event.quantity).quantize(self.quote_precision)
        self.log.debug(f'Match info: {event.quantity}@{event.price}')

        #self.balance.quote -= (event.quantity * event.price).quantize(self.quote_precision)

        #self.weight += event.quantity
        #self.active_funds -= (event.quantity * event.price).quantize(self.quote_precision)
        #self.notional_transacted += (event.quantity * event.price).quantize(self.quote_precision)

        self.log.info(f'Received match event for order {event.id}: {event.price}@{event.quantity}')
        self.log.debug(f'New inventory balance: {self.balance.inventory}')
        self.log.debug(f'Order {self.active_order.order_id} notional filled: {self.active_order.filled}')
        #self.log.debug(f'New weight: {self.weight}')

    def handle_done_event(self, event):
        if event.reason == 'canceled':
            #self.balance.quote += ((event.price * event.quantity).quantize(self.quote_precision) - self.active_order.filled).quantize(self.quote_precision)
            self.balance.quote += (event.price * event.remaining_size).quantize(self.quote_precision)
            self.log.debug(f'Finished cancelling order {event.id}. New quote balance: {self.balance.quote}')
            #self.active_funds -= (event.price * event.quantity - self.active_funds).quantize(self.quote_precision)
        elif event.reason == 'filled':
            self.log.debug(f'Finished filling order {event.id}.')
            #del self.orders[event.id]
        self.active_order = None
        self.state = self.STATE_RUN

        #self.active_funds -= (event.price * event.quantity - self.active_funds).quantize(self.quote_precision)

    def purchase_power(self, best_price):
        self.log.debug(f'[Purchase power] Quote: {self.balance.quote} Active: {self.active_funds}')
        #return self.balance.quote - self.active_funds
        return self.balance.quote

class AskBot(MakerBot):
    SIDE = 'ASK'

    def __init__(self, exchange_api, balance, pair, min_spread, unit_size):
        super().__init__(exchange_api, balance, pair, self.SIDE, min_spread, unit_size)

    def order(self, price, quantity):
        try:
            order = self.exchange_api.limit_sell_order(self.pair, price, quantity, post_only=True)
            self.active_funds += quantity
            return order
        except common.ExchangeException as e:
            self.balance.inventory += quantity
            raise e

    def increase_quote_by_min(self, price, ticks=1):
        '''
        Adjust the quote by the minimum allowed quantity, in a competitive direction
        '''
        return price - self.quote_precision * ticks

    def decrease_quote_by_min(self, price, ticks=1):
        '''
        Adjust the quote by the tick size, /away/ from the spread
        '''
        return price + self.quote_precision * ticks

    def better_price(self, p1, p2):
        '''
        Return true if p1 is a better bid than p2
        '''
        return p1 < p2

    def order_quantity(self, price):
        '''
        Return an order that, at smallest, is the minimum notional size. 
        '''
        #min_quantity = (self.notional_size / price).quantize(self.base_precision)
        #return max(min_quantity, min_quantity + self.weight)
        return self.unit_size

    # Order is an ActiveOrder tuple
    def cancel(self, order_id):
        #order = order.order
        try:
            self.exchange_api.cancel_order(order_id)
        except common.ExchangeException as e:
            raise e

    def purchase_power(self, best_price):
        return ((self.balance.inventory - self.active_funds) * best_price).quantize(self.quote_precision)

    def handle_match_event(self, event):
        # Determine if this was a complete match. If not, try to place a new order to meet the minimum
        # notional amount again, perhaps taking into account the new weighting.
        self.balance.inventory -= event.quantity
        self.balance.quote += (event.price * event.quantity).quantize(self.quote_precision)

        #self.weight -= event.quantity
        self.active_funds -= event.quantity
        #self.notional_transacted += (event.price * event.quantity).quantize(self.quote_precision)

        self.log.info(f'Received match event for order {event.id}: {event.price}@{event.quantity}')
        self.log.debug(f'New inventory balance: {self.balance.inventory}')
        #self.log.debug(f'New weight: {self.weight}')

    def handle_done_event(self, event):
        if event.reason == 'cancelled':
            self.log.debug(f'Finished cancelling order {event.id}. New inventory balance: {self.balance.inventory}')
            # Need to subtract the unfilled portion of the order
            self.active_funds -= (event.quantity - self.active_funds).quantize(self.base_precision)
        elif event.reason == 'filled':
            self.log.debug(f'Finished order {event.id}. Remaining orders: {self.orders.items()}')
            #del self.orders[event.id]

class FixedPrecision(int):
    def __new__(cls, value, prec):
        #value = int(value * 1/prec)
        obj = int.__new__(cls, value)
        obj.value = value
        obj.prec = prec
        return obj

    def __str__(self):
        return str(self.value * self.prec)

class AskSide():
    def __init__(self, pair, api):
        self.exchange_api = api
        self.pair = pair

    def order(self, price, quantity):
        return self.exchange_api.limit_sell_order(self.pair, price, quantity, post_only=True)

    def adjust_balance(self, balance, amount):
        balance.quote += amount

    def cancel(self, order_id):
        self.exchange_api.cancel_order(order_id)

    def compare_price(self, p1, p2):
        # 0 if equal, positive implies a better price, negative a worse price
        return (p1 - p2) * -1

class BidSide():
    def __init__(self, pair, api):
        self.exchange_api = api
        self.pair = pair

    def order(self, price, quantity):
        return self.exchange_api.limit_buy_order(self.pair, price, quantity, post_only=True)

    def adjust_balance(self, balance, amount):
        balance.inventory += amount

    def cancel(self, order_id):
        self.exchange_api.cancel_order(order_id)

    def compare_price(self, p1, p2):
        return p1 - p2

def calculate_bbo(best_bid, best_ask, min_spread):
    spread = best_ask - best_bid
    ticks = min_spread - spread

    if ticks != 0:
        return (FixedPrecision(best_bid - ceil(ticks/2), best_bid.prec), FixedPrecision(best_ask + floor(ticks/2), best_ask.prec))

    return (best_bid, best_ask)

async def main():
    log = logging.getLogger('mm')
    log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
    log.addHandler(handler)
    pair = 'REP/USD'

    exchange_name = 'coinbase'
    #exchange_api = BinanceAPI.BinanceAPI(util.read_auth_file('auth.json'))
    exchange_api = CoinbaseAPI.CoinbaseAPI(util.read_auth_file('auth.json'))

    pair_info = list(filter(lambda p: p.pair == exchange_api.convert_pair(pair), exchange_api.get_products()))[0]
    base, quote = pair.split('/')

    exchange = await trade.create_feed(exchange_name, pair)
    orderbook, ws = exchange[exchange_name]['ob'], exchange[exchange_name]['ws']
    log.info(f'Connected to {exchange_name} trade feed.')

    quote_balance = exchange_api.get_wallet_balance(quote).balance
    inventory_balance = exchange_api.get_wallet_balance(base).balance
    wallet = Balance(inventory_balance, quote_balance)

    log.info(f'{quote} balance: {quote_balance}')
    log.info(f'{base} balance: {inventory_balance}')

    #fees = exchange_api.get_fees()
    #log.info(f'Maker fee: {fees.maker_fee} Taker fee: {fees.taker_fee}')

    #askBot = MakerBot(lambda p, q: exchange_api.limit_sell_order(pair, p, q, post_only=True), lambda val: val - pair_info.quote_precision, lambda p1, p2: p1 < p2)
    #bidBot = MakerBot(lambda p, q: exchange_api.limit_buy_order(pair, p, q, post_only=True), lambda val: val + pair_info.quote_precision, lambda p1, p2: p1 > p2)

    bid_bot = BidBot(exchange_api, wallet, pair, 4, D(1.0))
    #ask_bot = AskBot(exchange_api, wallet, pair, pair_info.quote_precision * 4)

    cb_log = logging.getLogger('api.CoinbaseAPI')
    cb_log.setLevel(logging.DEBUG)

    side_api = { 'buy': BidSide(pair, exchange_api), 'sell': AskSide(pair, exchange_api) }
    active_bid, active_ask = None, None
    orders = {}

    qz_price = lambda p: D(p).quantize(pair_info.quote_precision)
    qz_quantity = lambda q: D(q).quantize(pair_info.base_precision)

    pticks = lambda price: int(price * (1/pair_info.quote_precision))
    qticks = lambda quantity: int(quantity * (1/pair_info.base_precision))
    tprice = lambda ticks: int(ticks * pair_info.quote_precision)
    tquantity = lambda ticks: int(ticks * pair_info.base_precision)

    # Programmatically set
    min_notional = 10
    transacted = 0
    inventory = int(wallet.inventory * (1/pair_info.base_precision))
    outstanding = 0

    log.debug(f'Starting inventory: {inventory}')

    bid_price, bid_quantity = None, None
    ask_price, ask_quantity = None, None

    async for event in ws:
        if event.event_type == 'orderbook_update':
            orderbook.update(event.update)
            bid_price, bid_quantity = orderbook.best_bid()
            ask_price, ask_quantity = orderbook.best_ask()
            #best_bid = common.BookEntry(*orderbook.best_bid())
            #best_ask = common.BookEntry(*orderbook.best_ask())
            #spread = D(best_ask.price - best_bid.price)

            # Normalize
            #bid_price = int(bid_price * 1/pair_info.quote_precision)
            #bid_quantity = int(bid_quantity * 1/pair_info.base_precision)
            bid_price = FixedPrecision(bid_price * (1/pair_info.quote_precision), pair_info.quote_precision)
            bid_quantity = FixedPrecision(bid_quantity * (1/pair_info.base_precision), pair_info.base_precision)

            #ask_price = int(ask_price * 1/pair_info.quote_precision)
            #ask_quantity = int(ask_quantity * 1/pair_info.base_precision)
            ask_price = FixedPrecision(ask_price * (1/pair_info.quote_precision), pair_info.quote_precision)
            ask_quantity = FixedPrecision(ask_quantity * (1/pair_info.base_precision), pair_info.base_precision)

            log.debug(f'Book BBO: {bid_price},{bid_quantity}/{ask_price},{ask_quantity} spread: {ask_price - bid_price}')
            our_bid, our_ask = calculate_bbo(bid_price, ask_price, 3)
            log.debug(f'Our BBO: {our_bid}/{our_ask}\n')

            # Initialize as named tuple
            prices = { 'buy': (our_bid, '1.00'), 'sell': (our_ask, '1.00') }
            opposite_side = { 'buy': 'sell', 'sell': 'buy' }

            # Order logic here 
            # We have access to the state variables here (including precision) so simply convert numbers to the correct
            # precision when placing orders.
            # Both variables will perform the same logic, performing calls through the respective api
            # What would the 'should_cancel()' condition for such an order be?
            if orders == {}:
                    for side in 'buy', 'sell':
                        try:
                            price, quantity = prices[side]
                            log.debug(f'Placing {side} order {quantity}@{price}')
                            order = side_api[side].order(*prices[side])
                            orders[order.order_id] = order
                        except common.ExchangeException as e:
                            log.debug(f'Failed to place order: {e.reason}')
            elif len(orders) == 1:
                order = list(orders.values())[0]
                # If the order placement given the current spread can be improved, cancel our old order. Replace next iteration
                if side_api[order.side].compare_price(order.price, prices[side][0]) < 0:
                    log.debug(f'Cancelling order: {order.order_id}. Better order placement available.')
                    try:
                        side_api[order.side].cancel(order.order_id)
                        del orders[order.order_id]
                    except common.ExchangeException as e:
                        log.debug(f'Failed to cancel order {order.order_id}. Reason: {e.reason}')

                    continue
                # Otherwise, just place an opposing order.
                else:
                    opp_side = opposite_side[order.side]
                    try:
                        price, quantity = prices[opp_side]
                        log.debug(f'Placing {opp_side} order {quantity}@{price}')
                        opp_order = side_api[opp_side].order(*prices[opp_side])
                        orders[opp_order.order_id] = opp_order
                    except common.ExchangeException as e:
                        log.debug(f'Failed to place matching order: {e.reason}')

                #order_price = orders[0].price
                # Override comparison operator, such that > implies better price 
                #side_api[order.side].price >

            # If we have two orders on the book, nothing else to do but wait


            #spread = bid_bot.handle_bbo(best_bid, best_ask, spread)
            #spread = ask_bot.handle_bbo(best_ask, spread)

        # Can't use side from the event -- need to lookup via order
        elif event.event_type == 'order_match':
            log.debug(f'Received fill {event.quantity}@{event.price} for order {event.id}')
            notional = pticks(event.price) * qticks(event.quantity)

            if event.side == 'buy':
                transacted -= notional
                outstanding += qticks(event.quantity)
                #inventory += qticks(event.quantity)
            elif event.side == 'sell':
                transacted += notional
                outstanding -= qticks(event.quantity)
                #inventory -= qticks(event.quantity)

            # If our outstanding price is negative, we sold more than we bought. So we'll have to buy back that inventory at the ask price
            # to close.
            outstanding_price = pticks(ask_price) if outstanding <= 0 else pticks(bid_price)

            # Subtract, as an ask is negative but results in a notional gain, whereas a bid is positive but is a notional loss
            log.debug(f'est profit: {transacted - outstanding_price * outstanding}')
            #if event.side == 'buy':
            #    bid_bot.handle_match_event(event)
            #else:
            #    ask_bot.handle_match_event(event)
            #log.info(f'Notional profit: {ask_bot.notional_transacted - bid_bot.notional_transacted}')
        elif event.event_type == 'order_done':
            if event.reason == 'filled':
                log.debug(f'Removing filled order: {event.id}')
                del orders[event.id]

            # Need to remove the order from orders
            #if event.side == 'buy':
            #    bid_bot.handle_done_event(event)
            #else:
            #    ask_bot.handle_done_event(event)
            #log.info(f'Notional profit: {ask_bot.notional_transacted - bid_bot.notional_transacted}')

if __name__ == '__main__':
    asyncio.run(main())
