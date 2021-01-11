import asyncio
import logging
import copy
from itertools import combinations
from collections import namedtuple
from decimal import Decimal, ROUND_DOWN
from timeit import default_timer as timer
import signal
import datetime
import heapq

import pandas as pd
import aiostream

from api import trade
from api import ob as simpleob
from api import CoinbaseAPI
from api import BinanceAPI
from api import PoloniexAPI
from api import common
from util import util, async_util
from util.util import RED, GREEN, CYAN, YELLOW, BLUE, BR_BLACK_BG, END

_log = logging.getLogger(__name__)
_trade_log = logging.getLogger('trade_log')

def color_exchange(name):
    return {
        'coinbase': f'{BLUE}coinbase{END}',
        'binance_us': f'{YELLOW}binance_us{END}',
        'poloniex': f'{CYAN}poloniex{END}'
    }[name]

# Include a & b budget for pair currency (maybe also include a pair field?) 
class ExchangeInfo():
    def __init__(self, exchange, api, ob, pair, asset, balance, base_precision, quote_precision, maker_fee, taker_fee, min_notional):
        self.exchange = exchange
        self.api = api
        self.ob = ob
        self.pair = pair
        self.asset = asset
        self.balance = balance
        self.base_precision = base_precision
        self.quote_precision= quote_precision
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.min_notional = min_notional

_EXCHANGES = {
    'coinbase': {
        'api': CoinbaseAPI.CoinbaseAPI
    },
    'binance_us': {
        'api': BinanceAPI.BinanceAPI
    },
    'poloniex': {
        'api': PoloniexAPI.PoloniexAPI
    }
}

class Strat():
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.a_pair = self.a.pair
        self.b_pair = self.b.pair

        self.quote_precision = min(self.a.quote_precision, self.b.quote_precision)
        self.base_precision = min(self.a.base_precision, self.b.base_precision)

        self.maker_fee = self.get_maker_fee()
        self.taker_fee = self.get_taker_fee()

        self.update()
    
    # Return a tuple (maker_name, maker_side, taker_name, taker_side)
    # ex: (poloniex, ASK, binance_us, BID)
    def info(self):
        raise NotImplementedError

    def update(self):
        self.maker_price, self.maker_quantity = self.get_maker_order()
        self.taker_price, self.taker_quantity = self.get_taker_order()
        self.quantity = self._quantity()

    # Necessary for orders that fill on a maker exchange, which are too small to take on the taker exchange
    # Think of it as an 'undo' operation, which occurs at a small loss.
    def liquidate_maker(self, market_price, quantity):
        raise NotImplementedError

    def liquidate(self, market_price, quantity):
        raise NotImplementedError

    def best_maker_price():
        raise NotImplementedError

    def _quantity(self):
        raise NotImplementedError

    def __lt__(self, other):
        return self.profit() < other.profit()

    def __gt__(self, other):
        return self.profit() > other.profit()

    def __le__(self, other):
        return self.profit() <= other.profit()

    def __ge__(self, other):
        return self.profit() >= other.profit()

    def __eq__(self, other):
        return self.profit() == other.profit()

    def __ne__(self, other):
        return self.profit() != other.profit()

    def get_maker_fee(self):
        raise NotImplementedError()

    def get_taker_fee(self):
        raise NotImplementedError()

    def get_maker_order(self):
        raise NotImplementedError()

    def get_taker_order(self):
        raise NotImplementedError()

    def maker_order(self):
        raise NotImplementedError()

    def taker_order(self):
        raise NotImplementedError()

    def cancel_make(self, order_id):
        raise NotImplementedError()

    def profit(self):
        raise NotImplementedError()

    def profitable(self):
        #return self.profit() * self.quantity > 0 and self.quantity * self.maker_price >= 10 and self.quantity * self.taker_price >= 10
        return self.profit() * self.quantity > 0

    def adjust_maker_balance(self, delta):
        raise NotImplementedError

    def adjust_taker_balance(self, delta):
        raise NotImplementedError

    def calculate_profit(self, maker_total, taker_total):
        raise NotImplementedError


class Strat1(Strat):
    '''
    Exchange a is the maker (buying coins), b is the taker (selling coins) for the bid side
    '''

    NAME = 'STRAT1'

    def __init__(self, a, b):
        super().__init__(a, b)
        self.maker = a
        self.taker = b

    def info(self):
        return (self.a.exchange, 'BUY', self.b.exchange, 'SELL')

    def get_maker_fee(self):
        return self.a.maker_fee

    def get_taker_fee(self):
        return self.b.taker_fee

    def get_maker_order(self):
        price, quantity = self.a.ob.best_bid()
        return price + self.a.quote_precision, quantity

    def get_taker_order(self):
        return self.b.ob.best_bid()

    def best_maker_price(self, price):
        return Decimal(price) >= self.a.ob.best_bid()[0]

    def liquidate_maker(self, market_price, quantity):
        return self.a.api.limit_sell_order(self.a_pair, (Decimal(market_price) * Decimal(.95)).quantize(self.a.quote_precision), Decimal(quantity).quantize(self.a.base_precision))

    def liquidate(self, market_price, quantity):
        return self.take(Decimal(market_price * Decimal(.95)).quantize(self.b.quote_precision), Decimal(quantity).quantize(self.b.base_precision))

    def make(self, price, quantity):
        return self.a.api.limit_buy_order(self.a_pair, price, quantity, post_only=True)

    def take(self, price, quantity):
        return self.b.api.limit_sell_order(self.b_pair, price, quantity, FOK=True)

    def maker_order(self):
        return self.a.api.limit_order(self.a.api.convert_pair(self.pair), self.maker_price, self.quantity, post_only=True)

    def taker_order(self):
        return self.b.api.market_sell_order(self.b.api.convert_pair(self.pair), self.quantity)

    def profit(self):
        spread = self.taker_price * (1 - self.taker_fee) - self.maker_price * (1 + self.maker_fee)
        if spread > 0 and (self.quantity * self.maker_price < self.maker.min_notional * Decimal(1.06) or self.quantity * self.taker_price < self.taker.min_notional * Decimal(1.06)):
            spread = -spread
        return spread * self.quantity

    def _quantity(self):
        return min(self.b.balance, self.b.ob.best_bid()[1]).quantize(max(self.a.base_precision, self.b.base_precision), rounding=ROUND_DOWN)

    def cancel_make(self, order_id):
        self.a.api.cancel_order(order_id, pair=self.a_pair)

    def adjust_maker_balance(self, delta):
        self.a.balance += Decimal(delta)

    def adjust_taker_balance(self, delta):
        self.b.balance -= Decimal(delta)

    def calculate_profit(self, maker_total, taker_total):
        # Eventually quantize according to exchange's USD precision
        return taker_total - maker_total

    def __str__(self):
        return f'STRAT1 Bid {self.maker_price}@{self.quantity} {self.a_pair} on {color_exchange(self.a.exchange)}, ' \
                f'Sell {self.taker_price}@{self.quantity} {self.b_pair} on {color_exchange(self.b.exchange)}'

class Strat2(Strat):
    '''
    Exchange b is the maker (buying coins), a is the taker (selling coins) for the bid side
    '''

    NAME = 'STRAT2'

    def __init__(self, a, b):
        super().__init__(a, b)
        self.maker = b
        self.taker = a

    def info(self):
        return (self.b.exchange, 'BUY', self.a.exchange, 'SELL')

    def get_maker_fee(self):
        return self.b.maker_fee

    def get_taker_fee(self):
        return self.a.taker_fee

    def get_maker_order(self):
        price, quantity = self.b.ob.best_bid()
        return price + self.b.quote_precision, quantity

    def get_taker_order(self):
        return self.a.ob.best_bid()
    
    def best_maker_price(self, price):
        return Decimal(price) >= self.b.ob.best_bid()[0]
    
    def liquidate_maker(self, market_price, quantity):
        return self.b.api.limit_sell_order(self.b_pair, (Decimal(market_price) * Decimal(.95)).quantize(self.b.quote_precision), Decimal(quantity).quantize(self.b.base_precision))

    def liquidate(self, market_price, quantity):
        return self.take(Decimal(market_price * Decimal(.95)).quantize(self.a.quote_precision), Decimal(quantity).quantize(self.a.base_precision))

    def make(self, price, quantity):
        return self.b.api.limit_buy_order(self.b_pair, price, quantity, post_only=True)

    def take(self, price, quantity):
        return self.a.api.limit_sell_order(self.a_pair, price, quantity, FOK=True)

    def maker_order(self):
        return self.b.api.limit_buy_order(self.b.api.convert_pair(self.pair), self.maker_price, self.quantity, post_only=True)

    def taker_order(self):
        return self.a.api.market_sell_order(self.a.api.convert_pair(self.pair), self.quantity)

    def profit(self):
        spread = self.taker_price * (1 - self.taker_fee) - self.maker_price * (1 + self.maker_fee)
        if spread > 0 and (self.quantity * self.maker_price < self.maker.min_notional * Decimal(1.06) or self.quantity * self.taker_price < self.taker.min_notional * Decimal(1.06)):
            spread = -spread
        return spread * self.quantity

    def _quantity(self):
        return min(self.a.balance, self.a.ob.best_bid()[1]).quantize(max(self.a.base_precision, self.b.base_precision), rounding=ROUND_DOWN)

    def cancel_make(self, order_id):
        self.b.api.cancel_order(order_id, pair=self.b_pair)

    def adjust_maker_balance(self, delta):
        self.b.balance += Decimal(delta)

    def adjust_taker_balance(self, delta):
        self.a.balance -= Decimal(delta)

    def __str__(self):
        return f'STRAT2 Bid {self.maker_price}@{self.quantity} {self.b_pair} on {color_exchange(self.b.exchange)}, ' \
                f'Sell {self.taker_price}@{self.quantity} {self.a_pair} on {color_exchange(self.a.exchange)}'

    def calculate_profit(self, maker_total, taker_total):
        return taker_total - maker_total

class Strat3(Strat):
    '''
    Exchange a is the maker (selling coins), exchange b is the taker (buying coins) for the ask side
    '''

    NAME = 'STRAT3'

    def __init__(self, a, b):
        super().__init__(a, b)
        self.maker = a
        self.taker = b

    def info(self):
        return (self.a.exchange, 'SELL', self.b.exchange, 'BUY')

    def get_maker_fee(self):
        return self.a.maker_fee

    def get_taker_fee(self):
        return self.b.taker_fee

    def get_maker_order(self):
        price, quantity = self.a.ob.best_ask()
        return price - self.a.quote_precision, quantity

    def get_taker_order(self):
        return self.b.ob.best_ask()

    def best_maker_price(self, price):
        return Decimal(price) <= self.a.ob.best_ask()[0]

    def liquidate_maker(self, market_price, quantity):
        return self.a.api.limit_buy_order(self.a_pair, (Decimal(market_price) * Decimal(1.05)).quantize(self.a.quote_precision), Decimal(quantity).quantize(self.a.base_precision))

    def liquidate(self, market_price, quantity):
        return self.take(Decimal(market_price * Decimal(1.05)).quantize(self.b.quote_precision), Decimal(quantity).quantize(self.b.base_precision))

    def make(self, price, quantity):
        return self.a.api.limit_sell_order(self.a_pair, price, quantity, post_only=True)

    def take(self, price, quantity):
        return self.b.api.limit_buy_order(self.b_pair, price, quantity, FOK=True)

    def maker_order(self):
        return self.a.api.limit_sell_order(self.a.api.convert_pair(self.pair), self.maker_price, self.quantity, post_only=True)

    def taker_order(self):
        return self.b.api.market_buy_order(self.b.api.convert_pair(self.pair), self.quantity)

    def profit(self):
        spread = self.maker_price * (1 - self.maker_fee) - self.taker_price * (1 + self.taker_fee)
        if spread > 0 and (self.quantity * self.maker_price < self.maker.min_notional * Decimal(1.06) or self.quantity * self.taker_price < self.taker.min_notional * Decimal(1.06)):
            spread = -spread
        return spread * self.quantity

    def _quantity(self):
        return min(self.a.balance, self.a.ob.best_ask()[1]).quantize(max(self.a.base_precision, self.b.base_precision), rounding=ROUND_DOWN)

    def cancel_make(self, order_id):
        self.a.api.cancel_order(order_id, pair=self.a_pair)

    def adjust_maker_balance(self, delta):
        self.a.balance -= Decimal(delta)

    def adjust_taker_balance(self, delta):
        self.b.balance += Decimal(delta)

    def __str__(self):
        return f'STRAT3 Ask {self.maker_price}@{self.quantity} {self.a_pair} on {color_exchange(self.a.exchange)}, ' \
                f'Buy {self.taker_price}@{self.quantity} {self.b_pair} on {color_exchange(self.b.exchange)}'

    def calculate_profit(self, maker_total, taker_total):
        return maker_total - taker_total

class Strat4(Strat):
    '''
    Exchange b is the maker (selling coins), exchange a is the taker (buying coins) for the ask side
    '''

    NAME = 'STRAT4'

    def __init__(self, a, b):
        super().__init__(a, b)
        self.maker = b
        self.taker = a

    def info(self):
        return (self.b.exchange, 'SELL', self.a.exchange, 'BUY')

    def get_maker_fee(self):
        return self.b.maker_fee

    def get_taker_fee(self):
        return self.a.taker_fee

    def get_maker_order(self):
        price, quantity = self.b.ob.best_ask()
        return price - self.b.quote_precision, quantity

    def get_taker_order(self):
        return self.a.ob.best_ask()

    def best_maker_price(self, price):
        return Decimal(price) <= self.b.ob.best_ask()[0]

    def liquidate_maker(self, market_price, quantity):
        return self.b.api.limit_buy_order(self.b_pair, (Decimal(market_price) * Decimal(1.05)).quantize(self.b.quote_precision), Decimal(quantity).quantize(self.b.base_precision))

    def liquidate(self, market_price, quantity):
        return self.take(Decimal(market_price * Decimal(1.05)).quantize(self.a.quote_precision), Decimal(quantity).quantize(self.a.base_precision))

    def make(self, price, quantity):
        return self.b.api.limit_sell_order(self.b_pair, price, quantity, post_only=True)

    def take(self, price, quantity):
        return self.a.api.limit_buy_order(self.a_pair, price, quantity, FOK=True)

    def maker_order(self):
        return self.b.api.limit_sell_order(self.a.api.convert_pair(self.pair), self.maker_price, self.quantity, post_only=True)

    def taker_order(self):
        return self.a.api.market_buy_order(self.b.api.convert_pair(self.pair), self.quantity)

    def profit(self):
        spread = self.maker_price * (1 - self.maker_fee) - self.taker_price * (1 + self.taker_fee)
        if spread > 0 and (self.quantity * self.maker_price < self.maker.min_notional * Decimal(1.06) or self.quantity * self.taker_price < self.taker.min_notional * Decimal(1.06)):
            spread = -spread
        return spread * self.quantity

    def _quantity(self):
        return min(self.b.balance, self.b.ob.best_ask()[1]).quantize(max(self.a.base_precision, self.b.base_precision), rounding=ROUND_DOWN)

    def cancel_make(self, order_id):
        self.b.api.cancel_order(order_id, pair=self.b_pair)

    def adjust_maker_balance(self, delta):
        self.b.balance -= Decimal(delta)

    def adjust_taker_balance(self, delta):
        self.a.balance += Decimal(delta)

    def __str__(self):
        return f'STRAT4 Ask {self.maker_price}@{self.quantity} {self.b_pair} on {color_exchange(self.b.exchange)}, ' \
                f'Buy {self.taker_price}@{self.quantity} {self.a_pair} on {color_exchange(self.a.exchange)}'

    def calculate_profit(self, maker_total, taker_total):
        return maker_total - taker_total

class Strat5(Strat):
    '''
    Exchange a & b are both takers for the bid side

    By maker it's implied that that exchange is the initial order (and hence, cheaper)
    So in reality we need to have a strategy for when a is the 'taker', and when b is the 'taker',
    or when a < b and b < a respectively..

    After all, that's why we have 4 strats for the above. On bids, need to handle a < b and b < a. On asks,
    need to handle a < b, and b < a; therefore, a total of 4 strats.

    The same must be said for the taker/taker strat. No market making occurs in this case. Rather, we have:
    The pattern is actually the same across strategies -- just depends on the side of the book

    Actually, how would taker/taker work? It requires an immediate purchase, that is, when:
    ask(a) < bid(b) ; Buy from a, Sell to b
    ask(b) < bid(a) ; Buy from b, Sell to a
    And of course, the symmetry holds
    bid(b) > ask(a)
    bid(a) > ask(b)

    If it's a taker/taker, and therefore 'instantaneous' across both exchanges, then it really doesn't matter which exchange we make the initial purchase on.
    Taker fees can differ of course

    For strat 5:
    ask(a) < bid(b) (a is our maker, b is our taker)

    In fact, we can reuse this strategy by merely swapping the order of arguments:
    Strat5(a,b)
    Strat6 = Strat5(b,a)

    Sell a, buy b
    Sell b, buy a
    '''

    NAME = 'STRAT5'

    '''
    Make will be an IOC order
    Take will be whatever was filled from that IOC order, as a market order (For now)
    '''
    def __str__(self):
        return f'STRAT5 Sell {BR_BLACK_BG}{self.maker_price}@{self.quantity:3.08f}{END} {self.a_pair} on {color_exchange(self.a.exchange)}, ' \
                f'Buy {BR_BLACK_BG}{self.taker_price}@{self.quantity:3.08f}{END} {self.b_pair} on {color_exchange(self.b.exchange)}'

    def __init__(self, a, b):
        super().__init__(a, b)

    def liquidate(self, market_price, quantity):
        pass

    def make(self, price, quantity):
        return self.a.api.limit_sell_order(self.a_pair, price, quantity, IOC=True)

    def take(self, price, quantity):
        return self.liquidate(price, quantity)

    # No cancellations for takers
    def cancel_make(self, order_id):
        pass

    # TODO -- Modify these methods to call the correct method
    def get_maker_fee(self):
        return self.a.taker_fee

    def get_taker_fee(self):
        return self.b.taker_fee

    # In this case, sell to the best bid, and buy from the best ask
    def get_maker_order(self):
        return self.a.ob.best_bid()

    def get_taker_order(self):
        return self.b.ob.best_ask()

    def maker_order(self):
        #maker_price, _ = self.get_maker_order()
        return self.a.api.market_buy_order(self.a.api.convert_pair(self.pair), self.maker_price, self.quantity, post_only=True)

    def taker_order(self):
        return self.b.api.market_sell_order(self.b.api.convert_pair(self.pair), self.quantity)

    def profit(self):
        #maker_price, _ = self.get_maker_order()
        #taker_price, _ = self.get_taker_order()
        #spread = self.taker_price * (1 - self.get_taker_fee()) - self.maker_price * (1 + self.get_maker_fee())
        spread = self.maker_price * (1 - self.get_maker_fee()) - self.taker_price * (1 + self.get_taker_fee())
        return spread * self.quantity

    def _quantity(self):
        return min(self.a.balance, self.a.ob.best_bid()[1])

class Strat6(Strat5):
    '''
    Exchange a & b are both takers for the ask side 
    '''

    NAME = 'STRAT6'

    def __str__(self):
        return f'STRAT6 Sell {BR_BLACK_BG}{self.maker_price}@{self.quantity:3.08f}{END} {self.a_pair} on {color_exchange(self.a.exchange)}, ' \
                f'Buy {BR_BLACK_BG}{self.taker_price}@{self.quantity:3.08f}{END} {self.b_pair} on {color_exchange(self.b.exchange)}'

    def __init__(self, a, b):
        super().__init__(b, a)

def init_exchanges(exchanges):
    info = {}
    auth = util.read_auth_file('auth.json')

    for exchange in exchanges:
        api = _EXCHANGES[exchange]['api'](auth)
        pair = exchanges[exchange]

        products = api.get_products()
        pair_product = products[list(map(lambda p: p.pair == api.convert_pair(pair), products)).index(True)]
        _log.info(f'{color_exchange(exchange)} precision - Base: {pair_product.base_precision} Quote: {pair_product.quote_precision} Min Notional: {pair_product.min_notional}')

        fees = api.get_fees()
        _log.info(f'{color_exchange(exchange)} fees - Maker: {fees.maker_fee} Taker: {fees.taker_fee}')

        wallet = api.get_wallet_balance(pair[:pair.index('/')])
        _log.info(f'{color_exchange(exchange)} {pair} balance - {wallet.balance} {wallet.asset}')

        info[exchange] = ExchangeInfo(exchange, api, None, pair, wallet.asset, wallet.balance, 
                pair_product.base_precision, pair_product.quote_precision, fees.maker_fee, fees.taker_fee, pair_product.min_notional)
        '''
        if exchange == 'poloniex':
            info[exchange] = ExchangeInfo(exchange, api, None, pair, wallet.asset, Decimal('0.01'), pair_product.base_precision, pair_product.quote_precision, Decimal(.0005), fees.taker_fee)
        else:
            info[exchange] = ExchangeInfo(exchange, api, None, pair, wallet.asset, Decimal('0.01'), pair_product.base_precision, pair_product.quote_precision, fees.maker_fee, fees.taker_fee)
        '''
        #info[exchange] = ExchangeInfo(exchange, api, None, pair, wallet.asset, Decimal('1.0'), pair_product.base_precision, pair_product.quote_precision, fees.maker_fee, fees.taker_fee)

    return info

async def stop(websockets):
    _log.info('Stopping mirror bot')
    tasks = [ task for task in asyncio.all_tasks() if task is not asyncio.current_task() ]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks)
    _log.info('Stopped all tasks')

async def main():
    #PAIR = 'LINK/USD'
    STATE_WAIT_FOR_ARB = 0
    STATE_WAIT_FOR_MATCH = 1
    STATE_CANCEL_MAKE = 2
    STATE_DONE = 3
    
    #exchange_list = ['coinbase', 'binance_us']
    #exchange_list = {'coinbase': 'ETC/USD', 'binance_us': 'ETC/USD'}
    #exchange_list = {'poloniex': 'ETC/USDC', 'binance_us': 'ETC/USD'}
    exchange_list = {'poloniex': 'ETH/USDC', 'binance_us': 'ETH/USD'}
    #exchange_list = {'poloniex': 'BCHABC/USDC', 'binance_us': 'BCH/USD'}
    #exchange_list = {'poloniex': 'USDT/USDC', 'binance_us': 'USDT/USD'} # Promising
    exchanges = init_exchanges(exchange_list)

    feeds = await trade.create_feeds(exchange_list)
    for exchange in exchange_list:
        exchanges[exchange].ob = feeds[exchange]['ob']
        #exchanges[exchange].pair = exchange_list[exchange]

    exchange_pairs = list(combinations(exchange_list, 2))
    current_strat = None
    strats = [ Strat(exchanges[a], exchanges[b]) for a, b in exchange_pairs for Strat in [ Strat1, Strat2, Strat3, Strat4 ] ]
    #strats = [ Strat(exchanges[a], exchanges[b]) for a, b in exchange_pairs for Strat in [ Strat5, Strat6 ] ]

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(stop([feeds[exchange]['ws'] for exchange in exchange_list])))
    state = STATE_WAIT_FOR_ARB
    taker_price = 0
    liquidation_price = 0
    maker_order = None
    taker_orders = {}
    profit = 0

    maker_total = 0
    taker_total = 0

    state_name = {
        STATE_WAIT_FOR_ARB:   'WAIT_FOR_ARB  ',
        STATE_WAIT_FOR_MATCH: 'WAIT_FOR_MATCH',
        STATE_CANCEL_MAKE:    'CANCEL_MAKE   ',
        STATE_DONE:           'DONE          '
    }

    async for event in aiostream.stream.merge(*[ exchange['ws'] for exchange in feeds.values() ]):
        #_log.info(event.event_type)
        if event.event_type == 'orderbook_update':
            feeds[event.exchange]['ob'].update(event.update)
            #if event.exchange == 'binance_us':
            #_log.info(f'BBO {feeds[event.exchange]["ob"].best_bid()}/{feeds[event.exchange]["ob"].best_ask()}')

            #_log.info(f'{color_exchange(event.exchange)} BBO - {exchanges[event.exchange].ob.best_bid()}/{exchanges[event.exchange].ob.best_ask()}')
            #_log.info(f'strats: {[strat.profit() for strat in strats]}')
            [ strat.update() for strat in strats ]

            if not current_strat:
                current_strat = max(strats)

            if current_strat.profitable():
                _log.info(f'{state_name[state]} {GREEN}(${current_strat.profit():2.10f}){END} - {current_strat}')
            else:
                _log.info(f'{state_name[state]} {RED}(${current_strat.profit():2.10f}){END} - {current_strat}')
                #current_strat = None
        # Primarily needed in the case of poloniex, which occasionally dispatches a new orderbook snapshot
        elif event.event_type == 'orderbook_snapshot':
            _log.info(f'{state_name[state]} Initializing new orderbook for exchange: {event.exchange}')
            feeds[event.exchange]['ob'] = simpleob.Orderbook(event.snapshot)
            exchanges[event.exchange].ob = feeds[event.exchange]['ob']
            continue

        # Run state machine here -- need to process order events.
        if state == STATE_WAIT_FOR_ARB:
            if current_strat and current_strat.profitable():
                _log.info(f'{state_name[state]} Placing order: {current_strat}')
                try:
                    maker_order = current_strat.make(current_strat.maker_price, current_strat.quantity)
                    taker_price = current_strat.taker_price
                    state = STATE_WAIT_FOR_MATCH
                except common.ExchangeException as e:
                    _log.error(f'{state_name[state]} Failed to place order: {e.reason}')
            else:
                current_strat = None

        elif state == STATE_WAIT_FOR_MATCH:
            if event.event_type == 'order_match':
                if event.id == maker_order.order_id:
                    # No matter what the fill is, we have to get rid of it. So liquidate.


                    current_strat.adjust_maker_balance(event.quantity)
                    maker_total += event.quantity * event.price
                    # May not post if the size is too small for the exchange
                    if event.price * event.quantity >= current_strat.taker.min_notional:
                        _log.info(f'{state_name[state]} Taker order: {taker_price}@{event.quantity} (original size: {maker_order.size})')
                        order = current_strat.liquidate(taker_price, event.quantity)
                        taker_orders[order.order_id] = order

                    if event.quantity < maker_order.size:
                        # Attempt to cancel the remainder of the order
                        try:
                            _log.info(f'{state_name[state]} Attempting to cancel remaining maker order')
                            current_strat.cancel_make(maker_order.order_id)
                        except common.ExchangeException as e:
                            _log.error(f'{state_name[state]} Failed to cancel remaining maker order: {e.reason}')

                    if event.price * event.quantity < current_strat.taker.min_notional:
                        if event.price * event.quantity() > current_strat.maker.min_notional * Decimal(1.06):
                            _log.info(f'{state_name[state]} Notional amount {event.quantity} too small, liquidating on maker.')
                            # Cancel out the adjustment
                            current_strat.adjust_maker_balance(-event.quantity)
                            current_strat.adjust_taker_balance(-event.quantity)
                            order = current_strat.liquidate_maker(event.price, event.quantity)
                            taker_orders[order.order_id] = order
                        else:
                            _log.info(f'{state_name[state]} Cannot liquidate notional amount {event.quantity} on maker; value is too small.')

                    state = STATE_CANCEL_MAKE
                    '''
                    if current_strat.profitable() and event.quantity <= current_strat.quantity:
                        _log.info(f'Submitting profitable taker order: {taker_price}@{event.quantity}')
                        order = current_strat.take(taker_price, event.quantity)
                        taker_orders[order.id] = order
                    else:
                        _log.info(f'Cancel order - Profitable: {current_strat.profitable()} Insufficient quantity: {current_strat.quantity < maker_order.size} Best price: {current_strat.best_maker_price(maker_order.price)}')
                        current_start.cancel_make(maker_order.order_id)
                        state = STATE_CANCEL_MAKE
                    '''
                '''
                elif event.id in taker_orders:
                    leftovers = taker_orders[event.id].size - event.quantity
                    if leftovers > 0:
                        _log.info('Liquidating remaining taker order {leftovers}')
                        order = current_strat.liquidate(taker_price, taker_orders[event.id].size - event.quantity)
                        taker_orders[order.id] = order
                '''

                '''
                elif event.event_type == 'order_done':
                    if event.id == maker_order.order_id:
                        _log.info('Maker order filled {maker_order.price}@{maker_order.size}')
                        # Enter a taker state
                        #state = STATE_DONE
                        maker_order = None
                        state = STATE_CANCEL_MAKE
                        continue
                    elif event.id in taker_orders:
                        _log.info('Taker order filled {taker_orders[event.id].size}')
                        del taker_orders[event.id]
                        continue
                '''

            # Alternatively, if all have is some sort of OB update, then perform the following check
            elif not current_strat.profitable() or current_strat.quantity < maker_order.size or not current_strat.best_maker_price(maker_order.price):
                #current_strat.cancel_make(maker_order.order_id)
                _log.info(f'{state_name[state]} Cancel order - Profitable: {current_strat.profitable()} Insufficient quantity: {current_strat.quantity < maker_order.size} Best price: {current_strat.best_maker_price(maker_order.price)}')

                try:
                    current_strat.cancel_make(maker_order.order_id)
                    state = STATE_CANCEL_MAKE
                except common.ExchangeException as e:
                    _log.error(f'{state_name[state]} Failed to cancel unprofitable maker order: {e.reason}')
                    state = STATE_CANCEL_MAKE

                #current_strat = None
            #else:
            #    _log.info(f'Waiting for match for order {order.id}')
        elif state == STATE_CANCEL_MAKE:
            if event.event_type == 'order_done':
                if maker_order and event.id == maker_order.order_id:
                    _log.info(f'{state_name[state]} Fully cancelled maker order: {event.reason}')
                    maker_order = None
                elif event.id in taker_orders:
                    # May have to add some extra handling if FOK fails (This filled, but not the maker ID)
                    _log.info(f'{state_name[state]} Fully filled taker order {taker_orders[event.id].price}@{taker_orders[event.id].size}: {event.reason}')
                    del taker_orders[event.id]
            elif event.event_type == 'order_match':
                if maker_order and event.id == maker_order.order_id:
                    _log.info(f'{state_name[state]} Received match ({event.quantity}) while cancelling maker order {maker_order.order_id}.')
                    current_strat.adjust_maker_balance(event.quantity)
                    maker_total += event.quantity * event.price
                    if event.quantity * taker_price >= current_strat.taker.min_notional:
                        _log.info(f'{state_name[state]} Liquidating')
                        order = current_strat.liquidate(taker_price, event.quantity)
                        taker_orders[order.order_id] = order
                    else:
                        if event.quantity * event.price > current_strat.maker.min_notional * Decimal(1.06):
                            _log.info(f'{state_name[state]} Liquidating fill of size {event.quantity} on maker; notional value too small')
                            order = current_strat.liquidate_maker(event.price, event.quantity)
                            taker_orders[order.order_id] = order
                            current_strat.adjust_maker_balance(-event.quantity)
                            current_strat.adjust_taker_balance(-event.quantity)
                        else:
                            _log.info(f'{state_name[state]} Cannot liquidate, quantity {event.quantity} too small for maker exchange')

                elif event.id in taker_orders:
                    current_strat.adjust_taker_balance(event.quantity)
                    taker_total += event.quantity * event.price
                '''
                elif event.id in taker_orders:
                    _log.info(f'Received taker fill: {event.quantity}')
                    leftover = taker_orders[event.id].size - event.quantity
                    if leftover > 0:
                        _log.info(f'Attempting to liquidate remainder: {leftover}')
                        order = current_strat.liquidate(taker_price, leftover)
                        self.taker_orders[order.order_id] = order
                '''

            # Completely cleaned up all maker orders and taker orders
            if not maker_order and taker_orders == {}:
                profit = current_strat.calculate_profit(maker_total, taker_total)
                if profit > 0 or maker_total > 0:
                    maker_ex, maker_side, taker_ex, taker_side = current_strat.info()
                    _trade_log.info(f'{datetime.datetime.now().isoformat()},{current_strat.NAME},{maker_ex},{maker_side},{taker_ex},{taker_side},{maker_total},{taker_total},{profit}')

                _log.info(f'{state_name[state]} Order completely cleaned up. Profit: {profit}')
                current_strat = None
                maker_total = 0
                taker_total = 0
                #state = STATE_DONE
                state = STATE_WAIT_FOR_ARB
                #state = STATE_DONE
            #else:
            #    _log.info(f'Waiting for maker order {maker_order.order_id} to cancel')
        elif state == STATE_DONE:
            pass
            #_log.info('Done')

if __name__ == '__main__':
    _log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    _log.addHandler(handler)

    fh = logging.FileHandler('trades.csv')
    _trade_log.addHandler(fh)
    _trade_log.setLevel(logging.INFO)

    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        _log.info('Bye!')
