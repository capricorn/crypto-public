import asyncio
import json
import datetime
import sys
import logging
from functools import reduce

import requests
import websockets
import CoinbaseAPI as cbapi
import KrakenAPI as kapi
import ob as simpleob
import pandas as pd
from exchange import Exchange, UnknownOrderException
from fp import FixedPrecision

#KRAKEN_ID = 'REP/EUR'
KRAKEN_PAIR = 'REPUSD'
KRAKEN_ID = 'REP/USD'
COINBASE_ID = 'REP-USD'
#KRAKEN_ID = 'XRP/EUR'
#COINBASE_ID = 'XRP-USD'
#KRAKEN_ID = 'ETH/EUR'
#COINBASE_ID = 'ETH-USD'
#KRAKEN_ID = 'BTC/EUR'
#COINBASE_ID = 'BTC-USD'

coinbase = {}
kraken = {}

data = Exchange.read_auth_file('auth.json')
krak = kapi.KrakenAPI(data)
cb = cbapi.CoinbaseAPI(data)
ex = Exchange('auth.json')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt='[%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Make sure our bestask or bestbid doesn't enter the otherside of the book? (aka filled immediately)
# post only prevents this, but we'll need some error handling
class BotState():
    BID = 0
    ASK = 1
    STATE_WAITING_FOR_ARB = 0
    STATE_WAITING_FOR_MATCH = 1
    STATE_STOP = 2

    # Right now, we have 1 coin on each side, 1 for the ask book, and 1 for the bid book
    def __init__(self, bot_type, budget=100, coins=2):
        self.state = BotState.STATE_WAITING_FOR_ARB
        self.type = bot_type
        self.budget = budget
        self.coins = coins
        self.quantity = 0
        self.maker_price = 0
        self.taker_price = 0
        self.price = 0
        self.order_id = None
        self.maker = None
        self.balance = {
            'kraken': coins / 2,
            'coinbase': coins / 2
        }
        
    # Returns (maker_fees, taker_fees)
    def _get_fees(self, maker):
        if maker == 'kraken':
            return (.0016, .0025)
        if maker == 'coinbase':
            return (.0015, .0026)

    def _get_best_bid_side(self, kraken_best_bid, coinbase_best_bid):
        if kraken_best_bid < coinbase_best_bid:
            return ('kraken', kraken_best_bid, coinbase_best_bid)
        else:
            return ('coinbase', coinbase_best_bid, kraken_best_bid)

    def _get_best_ask_side(self, kraken_best_ask, coinbase_best_ask):
        if kraken_best_ask > coinbase_best_ask:
            return ('kraken', kraken_best_ask, coinbase_best_ask)
        else:
            return ('coinbase', coinbase_best_ask, kraken_best_ask)

    def _get_best_bid(self, exchange_data):
        return list(map(lambda k: round(float(k), simpleob.precision(k)), exchange_data['best_bid']))

    def _get_best_ask(self, exchange_data):
        return list(map(lambda k: round(float(k), simpleob.precision(k)), exchange_data['best_ask']))

    # Goal is to place asks (sell) on the higher priced book, and then buy back on the lower priced book
    def _handle_ask(self, coinbase_data, kraken_data):
        maker_match = {
            'kraken': None,
            'coinbase': None
        }

        if 'match' in kraken_data:
            maker_match['kraken'] = True
        if 'match' in coinbase_data:
            maker_match['coinbase'] = True

        coinbase_best_ask, coinbase_best_ask_vol = self._get_best_ask(coinbase_data)
        kraken_best_ask, kraken_best_ask_vol = self._get_best_ask(kraken_data)

        if self.state == BotState.STATE_WAITING_FOR_ARB:
            maker, maker_ask, taker_ask = self._get_best_ask_side(kraken_best_ask, coinbase_best_ask)
            maker_fee, taker_fee = self._get_fees(maker)
            unit_price = 10**(-Exchange.get_price_precision(maker, 'REP/USD'))
            # Need to check here if we're going to accidentally cross the book (need to see best bid) 
            if (maker_ask - self._get_best_bid(kraken_data if maker == 'kraken' else coinbase_data)[0]) > unit_price:
                maker_ask -= unit_price
            maker_ask = round(maker_ask, Exchange.get_price_precision(maker, 'REP/USD'))

            profit = (maker_ask - taker_ask) - maker_ask*maker_fee - taker_ask*taker_fee
            #max_quantity = min(self.balance['kraken'], self.balance['coinbase'], kraken_best_ask_vol, coinbase_best_ask_vol)
            #max_quantity = min(self.balance['kraken'], self.balance['coinbase'], kraken_best_ask_vol, coinbase_best_ask_vol)
            max_quantity = min(self.balance[maker], kraken_best_ask_vol if maker == 'coinbase' else coinbase_best_ask_vol)
            print(f'{datetime.datetime.now().isoformat()} (ASK) maker: {maker} makerbid: {maker_ask} takerbid: {taker_ask} profit: {profit*max_quantity} quantity: {max_quantity:.{Exchange.get_volume_precision(maker, "REP/USD")}f}')
            # If the quantity is too small, but it's profitable, buy up the higher up items in the OB
            if profit > 0 and max_quantity > 0.1:   # Avoid hardcoding in the future
                print(f'(ASK) Placing ask on [{maker}]')
                self.price = maker_ask
                self.quantity = round(max_quantity, Exchange.get_volume_precision(maker, 'REP/USD'))
                print(f'(ASK) Profit: {self.quantity}@{maker_ask} = {profit*self.quantity}')
                self.maker = maker
                # Set order id here
                self.order_id = ex.limit_sell_order(maker, 'REP/USD', self.price, self.quantity, post_only=True)
                self.state = BotState.STATE_WAITING_FOR_MATCH
                #self.state = BotState.STATE_STOP
        elif self.state == BotState.STATE_WAITING_FOR_MATCH:
            print('Waiting for match')
            if maker_match[self.maker]:
                # Here we can do a separate check, depending on if the match was coinbase or kraken
                # Also need to know the exact amount filled, since it may not have been the entire order

                print('(ASK) Matched!')
                #self.balance[self.maker] -= self.quantity

                filled_amount = 0
                # Likewise, need to check if the match is actually ours (need to check volume executed too; be careful with this)
                if self.maker == 'coinbase' and coinbase_data['match']['maker_order_id'] == self.order_id:
                    filled_amount = coinbase_data['match']['size']
                    self.balance['coinbase'] -= float(filled_amount)
                elif self.maker == 'kraken':
                    for match in kraken_data['match']:
                        price,_,_,side,_,_ = match
                        if float(price) == self.price and side == 'sell': # buy, for bid side? May need to double check
                            # If our order id doesn't show up in orders, then the entire order was filled
                            orders = krak.get_open_orders()
                            if self.order_id in orders:
                                # Theoretically needs rounded
                                filled_amount = orders[self.order_id]['vol_exec']
                            else:
                                filled_amount = self.quantity

                            self.balance['kraken'] -= float(filled_amount)

                '''
                elif self.maker == 'kraken' and self._check_kraken_trade_filled(self.order_id):
                    # If our order id doesn't show up in orders, then the entire order was filled
                    orders = krak.get_open_orders()
                    if order_id in orders:
                        # Theoretically needs rounded
                        filled_amount = orders[order_id]['vol_exec']
                    else:
                        filled_amount = self.quantity

                    self.balance['kraken'] -= float(filled_amount)
                '''

                if float(filled_amount) != 0:
                    try:
                        # Theoretically, part of our order could fill here, which would be bad
                        ex.cancel_order(self.maker, self.order_id)
                    except UnknownOrderException:
                        pass
                    # Cancel the remaining part of the trade (if it exists)
                    # Perform opposite market order (in this case, buy market order on opposite exchange)
                    # Would be nice to also record what the trade actually resolved as (avg price, quantity, etc)
                    # (I think market buy order should return this information)
                    market = 'kraken' if self.maker == 'coinbase' else 'coinbase'
                    print(f'Placing market buy order for {filled_amount} on [{market}]')
                    ex.market_buy_order(market, 'REP/USD', filled_amount)
                    self.balance[market] += float(filled_amount)    # Not totally accurate
                    self.state = BotState.STATE_STOP
                    return
                #self.state = BotState.STATE_WAITING_FOR_ARB

            # Need to calculate these
            # Profit should be calculated using our current ask position (self.price)
            maker_fee, taker_fee = self._get_fees(self.maker)
            best_ask, best_mm_ask = (coinbase_best_ask, kraken_best_ask) if self.maker == 'kraken' else (kraken_best_ask, coinbase_best_ask)
            # Would be the smallest quantity available in any balance, or the smallest ask quantity on both exchanges
            #max_quantity = min(self.balance['kraken'], self.balance['coinbase'], kraken_best_ask_vol, coinbase_best_ask_vol)
            max_quantity = min(self.balance[self.maker], kraken_best_ask_vol if self.maker == 'coinbase' else coinbase_best_ask_vol)
            profit = (self.price - best_ask) - self.price*maker_fee - best_ask*taker_fee

            if profit < 0 or best_mm_ask < self.price or max_quantity < self.quantity:
                try:
                    print('Cancelling order.')
                    print(f'Profit: {profit}')
                    print(f'best_mm_ask ({best_mm_ask}) < self.price ({self.price}): {best_mm_ask > self.price}')
                    print(f'max_quantity ({max_quantity}) < self.quantity ({self.quantity}): {max_quantity < self.quantity}')
                    ex.cancel_order(self.maker, self.order_id)
                    self.state = BotState.STATE_WAITING_FOR_ARB
                except UnknownOrderException:
                    # Means a complete fill occurred; do opposite action on other exchange
                    # May want to consider what to do if a partial fill happened?
                    print('Couldn\'t cancel order..')
                    self.state = BotState.STATE_WAITING_FOR_ARB

        elif self.state == BotState.STATE_STOP:
            print('(ASK) Stopping bot')
            print(f'Kraken balance: {self.balance["kraken"]}')
            print(f'Coinbase balance: {self.balance["coinbase"]}')

    # Need to decide how to split coins for bid budget and ask budget
    # Don't want to accidentally trade with coins that we don't have
    def _handle_bid(self, coinbase_data, kraken_data):
        maker_match = {
            'kraken': None,
            'coinbase': None
        }

        if 'match' in kraken_data:
            maker_match['kraken'] = True
        if 'match' in coinbase_data:
            maker_match['coinbase'] = True

        coinbase_best_bid, coinbase_best_bid_vol = self._get_best_bid(coinbase_data)
        kraken_best_bid, kraken_best_bid_vol = self._get_best_bid(kraken_data)

        if self.state == BotState.STATE_WAITING_FOR_ARB:
            maker, maker_bid, taker_bid = self._get_best_bid_side(kraken_best_bid, coinbase_best_bid)
            taker = 'kraken' if maker == 'coinbase' else 'coinbase'
            taker_vol = kraken_best_bid_vol if taker == 'kraken' else coinbase_best_bid_vol
            maker_fee, taker_fee = self._get_fees(maker)
            unit_price = 10**(-Exchange.get_price_precision(maker, 'REP/USD'))
            # Need to check here if we're going to accidentally cross the book (need to see best bid) 
            if (self._get_best_ask(kraken_data if maker == 'kraken' else coinbase_data)[0] - maker_bid) > unit_price:
                maker_bid += unit_price
            maker_bid = round(maker_bid, Exchange.get_price_precision(maker, 'REP/USD'))

            profit = (taker_bid - maker_bid) - maker_bid*maker_fee - taker_bid*taker_fee
            #max_quantity = min(self.balance['kraken'], self.balance['coinbase'], kraken_best_ask_vol, coinbase_best_ask_vol)
            #max_quantity = min(self.balance['kraken'], self.balance['coinbase'], kraken_best_bid_vol, coinbase_best_bid_vol)
            max_quantity = min(self.balance[taker], taker_vol)
            print(f'{datetime.datetime.now().isoformat()} (BID) maker: {maker} makerbid: {maker_bid} takerbid: {taker_bid} profit: {profit} quantity: {max_quantity:.{Exchange.get_volume_precision(maker, "REP/USD")}f}')
            # If the quantity is too small, but it's profitable, buy up the higher up items in the OB
            if profit > 0 and max_quantity > 0.1:   # Avoid hardcoding in the future
                print(f'(BID) Placing bid on [{maker}]')
                self.price = maker_bid
                self.quantity = round(max_quantity, Exchange.get_volume_precision(maker, 'REP/USD'))
                print(f'(BID) Profit: {self.quantity}@{self.price} = {profit*self.quantity}')
                self.maker = maker
                # Set order id here
                self.order_id = ex.limit_buy_order(maker, 'REP/USD', self.price, self.quantity, post_only=True)
                self.state = BotState.STATE_WAITING_FOR_MATCH
                #self.state = BotState.STATE_STOP
        elif self.state == BotState.STATE_WAITING_FOR_MATCH:
            print('(BID) Waiting for match')
            if maker_match[self.maker]:
                # Here we can do a separate check, depending on if the match was coinbase or kraken
                # Also need to know the exact amount filled, since it may not have been the entire order

                print(f'(BID) Checking match on [{self.maker}]')
                #print('(BID) Matched!')
                #self.balance[self.maker] -= self.quantity

                filled_amount = 0
                # Likewise, need to check if the match is actually ours (need to check volume executed too; be careful with this)
                if self.maker == 'coinbase' and coinbase_data['match']['maker_order_id'] == self.order_id:
                    filled_amount = coinbase_data['match']['size']
                    self.balance['coinbase'] += float(filled_amount) # Needs properly rounded?
                #elif self.maker == 'kraken' and self._check_kraken_trade_filled(self.order_id):
                elif self.maker == 'kraken':
                    for match in kraken_data['match']:
                        price,_,_,side,_,_ = match
                        if float(price) == self.price and side == 'buy': # buy, for bid side? May need to double check
                            # If our order id doesn't show up in orders, then the entire order was filled
                            orders = krak.get_open_orders()
                            if self.order_id in orders:
                                # Theoretically needs rounded
                                filled_amount = orders[self.order_id]['vol_exec']
                            else:
                                filled_amount = self.quantity

                            self.balance['kraken'] += float(filled_amount)

                if float(filled_amount) != 0:
                    # This should probably happen after, sense it adds latency
                    try:
                        # Theoretically, part of our order could fill here, which would be bad
                        ex.cancel_order(self.maker, self.order_id)
                    except UnknownOrderException:
                        pass
                    # Cancel the remaining part of the trade (if it exists)
                    # Perform opposite market order (in this case, buy market order on opposite exchange)
                    # Would be nice to also record what the trade actually resolved as (avg price, quantity, etc)
                    # (I think market buy order should return this information)
                    market = 'kraken' if self.maker == 'coinbase' else 'coinbase'
                    print(f'(BID) Placing market sell order for {filled_amount} on [{market}]')
                    ex.market_sell_order(market, 'REP/USD', filled_amount)
                    self.balance[market] -= float(filled_amount)    # Not totally accurate
                    self.state = BotState.STATE_STOP
                    return
                #self.state = BotState.STATE_WAITING_FOR_ARB

            # Need to calculate these
            # Profit should be calculated using our current ask position (self.price)
            maker_fee, taker_fee = self._get_fees(self.maker)
            best_bid, best_mm_bid = (coinbase_best_bid, kraken_best_bid) if self.maker == 'kraken' else (kraken_best_bid, coinbase_best_bid)
            # Would be the smallest quantity available in any balance, or the smallest ask quantity on both exchanges
            #max_quantity = min(self.balance['kraken'], self.balance['coinbase'], kraken_best_bid_vol, coinbase_best_bid_vol)
            max_quantity = kraken_best_bid_vol if self.maker == 'coinbase' else coinbase_best_bid_vol
            #profit = (self.price - best_ask) - self.price*maker_fee - best_ask*taker_fee
            profit = (best_bid - self.price) - self.price*maker_fee - best_bid*taker_fee

            #if profit < 0 or best_mm_bid > self.price or max_quantity < self.quantity:
            if profit < 0 or best_mm_bid > self.price or self.quantity > max_quantity:
                try:
                    print('(BID) Cancelling order.')
                    print(f'(BID) Profit: {profit}')
                    print(f'(BID) best_mm_bid ({best_mm_bid}) > self.price ({self.price}): {best_mm_bid > self.price}')
                    print(f'(BID) max_quantity ({max_quantity}) < self.quantity ({self.quantity}): {max_quantity < self.quantity}')
                    ex.cancel_order(self.maker, self.order_id)
                    self.state = BotState.STATE_WAITING_FOR_ARB
                except UnknownOrderException:
                    # Means a complete fill occurred; do opposite action on other exchange
                    # May want to consider what to do if a partial fill happened?
                    print('(BID) Couldn\'t cancel order..')
                    self.state = BotState.STATE_WAITING_FOR_ARB

        elif self.state == BotState.STATE_STOP:
            print('(BID) Stopping bot')
            print(f'(BID) Kraken balance: {self.balance["kraken"]}')
            print(f'(BID) Coinbase balance: {self.balance["coinbase"]}')


    # still worth attempting to cancel the trade, incase it was partially filled
    #def _check_kraken_trade_filled(self, order_id):
    #    orders = krak.get_open_orders(order_id)
    #    return order_id in orders and float(orders[order_id]['vol_exec']) > 0

    # We need to know which exchange is our mm exchange, and which is our taker exchange.
    '''
    def _handle_bid(self, coinbase_data, kraken_data):
        kraken_best_bid, kraken_best_bid_vol = self._get_best_bid(kraken_data)
        coinbase_best_bid, coinbase_best_bid_vol = self._get_best_bid(coinbase_data)

        #print(f'KBB: {kraken_best_bid} CBB: {coinbase_best_bid}')

        if self.state == BotState.STATE_WAITING_FOR_ARB:
            maker, maker_bid, taker_bid = self._get_best_bid_side(kraken_best_bid, coinbase_best_bid)
            maker_fee, taker_fee = self._get_fees(maker)
            unit_price = 10**(-Exchange.get_price_precision(maker, 'REP/USD'))
            if (maker_bid - self._get_best_ask(kraken_data if maker == 'kraken' else coinbase_data)[0]) > unit_price:
                maker_bid += unit_price
            maker_bid = round(maker_bid, Exchange.get_price_precision(maker, 'REP/USD'))

            profit = (taker_bid - maker_bid) - taker_bid*(taker_fee) - maker_bid*(maker_fee)
            print(f'{datetime.datetime.now().isoformat()} (BID) maker: {maker} makerbid: {maker_bid} takerbid: {taker_bid} profit: {profit}')
            max_quantity = min(kraken_best_bid_vol, coinbase_best_bid_vol, (self.budget*(1-maker_fee))/maker_bid)
            if profit > 0:
                self.quantity = round(max_quantity, Exchange.get_volume_precision(maker, 'REP/USD'))
                print(f'(BID) Placing bid on [{maker}]')
                print(f'(BID) Profit: {self.quantity}@{maker_bid} = {round(profit*self.quantity, Exchange.get_price_precision(maker, "REP/USD"))}')
                self.price = maker_bid
                self.maker = maker
                #order_id = ex.limit_buy_order('coinbase', 'BTC/USD', self.price, self.quantity, sandbox=True)
                #self.order_id = order_id
                self.state = BotState.STATE_WAITING_FOR_MATCH
                #self.state = BotState.STATE_STOP
        elif self.state == BotState.STATE_WAITING_FOR_MATCH:
            # This is where various cancellations will occur, according to price changes, etc
            # We should know the maker, so use that to see if our position still checks out.
            # Check if profit still exists according to the current state of the orderbook?
            #print('Waiting for match')
            print('(BID) Got match!')
            self.state = BotState.STATE_WAITING_FOR_ARB
            return
            # Need to check:
            # 1. If profit is gone
            # 2. If latest best bid is better than ours
            # 3. If max_quantity has lowered from our current order
            maker_fee, taker_fee = self._get_fees(self.maker)
            max_quantity = min(kraken_best_bid_vol, coinbase_best_bid_vol, (self.budget*(1-maker_fee))/self.price)
            profit = 0
            best_bid = 0
            if self.maker == 'kraken':
                profit = (coinbase_best_bid - kraken_best_bid) - coinbase_best_bid*taker_fee - kraken_best_bid*maker_fee
                best_bid = kraken_best_bid
            elif self.maker == 'coinbase':
                profit = (kraken_best_bid - coinbase_best_bid) - coinbase_best_bid*maker_fee - kraken_best_bid*taker_fee
                best_bid = coinbase_best_bid

            # Cancel order in all of these cases
            # Need to eventually deal with the fact that orders may fill before we can cancel them
            if profit < 0:
                print('Cancelling order, profit disappeared')
                ex.cancel_order('coinbase', self.order_id, sandbox=True)
                self.state = BotState.STATE_WAITING_FOR_ARB
            elif best_bid > self.price:
                print('Cancelling order, better bid than us')
                ex.cancel_order('coinbase', self.order_id, sandbox=True)
                self.state = BotState.STATE_WAITING_FOR_ARB
            elif max_quantity < self.quantity:
                print('Cancelling order, available quantity decreased')
                ex.cancel_order('coinbase', self.order_id, sandbox=True)
                self.state = BotState.STATE_WAITING_FOR_ARB
        elif self.state == BotState.STATE_STOP:
            print('Bot stopped.')

    '''

    # Perform all trading, including adjustments, here
    def handle_data(self, coinbase_data, kraken_data):
        kraken_best_bid, kraken_best_bid_vol = self._get_best_bid(kraken_data)
        coinbase_best_bid, coinbase_best_bid_vol = self._get_best_bid(coinbase_data)

        kraken_best_ask, kraken_best_ask_vol = self._get_best_ask(kraken_data)
        coinbase_best_ask, coinbase_best_ask_vol = self._get_best_ask(coinbase_data)

        coinbase_taker_fee = .0025
        coinbase_maker_fee = .0015
        kraken_taker_fee = .0026
        kraken_maker_fee = .0016

        pair = 'REP/USD'

        # Options
        # strat0: Bid: MM (buy) on kraken, taker (sell) on CB
        # strat1: Bid: MM (buy) on coinbase, taker (sell) on kraken
        # strat2: Ask: MM (sell) on kraken, taker (buy) on CB
        # strat3: Ask: MM (sell) on coinbase, taker (buy) on kraken
        # strat4: Instant-buy on coinbase, Instant-sell on kraken (fill or kill?)
        # strat5: Instant-buy on kraken, instant-sell on coinbase

        # TODO
        # Need to hardcode smallest units
        # Utilize fixed precision somehow
        # Make generic, so we can provide a list of exchange names, and generate the strategy? (Provides us more trading options)
        makers = [
            'kraken',
            'coinbase',
            'kraken',
            'coinbase'
        ]

        takers = [
            'coinbase',
            'kraken',
            'coinbase',
            'kraken'
        ]

        maker_prices = [
            kraken_best_bid + .001,
            coinbase_best_bid + .01,
            kraken_best_ask - .001,
            coinbase_best_ask - .01,
            coinbase_best_ask,
            kraken_best_ask
        ]

        taker_prices = [
            coinbase_best_bid,
            kraken_best_bid,
            coinbase_best_ask,
            kraken_best_ask,
            kraken_best_bid,
            coinbase_best_bid
        ]

        strat_spreads = [
            (coinbase_best_bid - (kraken_best_bid + .001)) - (kraken_best_bid + .001)*kraken_maker_fee - coinbase_best_bid*coinbase_taker_fee,
            (kraken_best_bid - (coinbase_best_bid + .01)) - (coinbase_best_bid + .01)*coinbase_maker_fee - kraken_best_bid*kraken_taker_fee,
            ((kraken_best_ask - .001) - coinbase_best_ask) - (kraken_best_ask - .001)*kraken_maker_fee - coinbase_best_ask*coinbase_taker_fee,
            ((coinbase_best_ask - .01) - kraken_best_ask) - (coinbase_best_ask - .01)*coinbase_maker_fee - kraken_best_ask*kraken_taker_fee,
            (kraken_best_bid - coinbase_best_ask) - kraken_best_bid*kraken_taker_fee - coinbase_best_ask*coinbase_taker_fee,
            (coinbase_best_bid - kraken_best_ask) - coinbase_best_bid*coinbase_taker_fee - kraken_best_ask*kraken_taker_fee
        ]

        strat_volume = [
            min(self.balance['coinbase'], coinbase_best_bid_vol),
            min(self.balance['kraken'], kraken_best_bid_vol),
            min(self.balance['kraken'], coinbase_best_ask_vol),
            min(self.balance['coinbase'], kraken_best_ask_vol),
            min(self.balance['kraken'], kraken_best_bid_vol),
            min(self.balance['coinbase'], coinbase_best_bid_vol)
        ]

        maker_functions = [
            lambda price, volume: ex.limit_buy_order('kraken', pair, price, volume, post_only=True),
            lambda price, volume: ex.limit_buy_order('coinbase', pair, price, volume, post_only=True),
            lambda price, volume: ex.limit_sell_order('kraken', pair, price, volume, post_only=True),
            lambda price, volume: ex.limit_sell_order('coinbase', pair, price, volume, post_only=True),
            # Eventually enable fill or kill? (or should we just do market orders?)
            lambda price, volume: ex.limit_buy_order('coinbase', pair, price, volume),
            lambda price, volume: ex.limit_buy_order('kraken', pair, price, volume)
        ]

        taker_functions = [
            lambda price, volume: ex.market_sell_order('coinbase', pair, volume),
            lambda price, volume: ex.market_sell_order('kraken', pair, volume),
            lambda price, volume: ex.market_buy_order('coinbase', pair, volume),
            lambda price, volume: ex.market_buy_order('kraken', pair, volume),
            # Eventually enable fill or kill?
            lambda price, volume: ex.limit_sell_order('kraken', pair, volume),
            lambda price, volume: ex.limit_sell_order('coinbase', pair, volume),
        ]

        # Unnecessary for our taker/taker strats
        better_maker_functions = [
            lambda our_bid, best_bid: (best_bid - .001) > our_bid,
            lambda our_bid, best_bid: (best_bid - .01) > our_bid,
            lambda our_ask, best_ask: (best_ask + .001) < our_ask,
            lambda our_ask, best_ask: (best_ask + .01) < our_ask
        ]

        profits = [ spread*volume for spread, volume in zip(strat_spreads, strat_volume) ]
        best_strat = list(map(lambda k: k == max(profits), profits)).index(True)
        maker, taker = maker_functions[best_strat], taker_functions[best_strat]

        # First time around, state would be set for strat, price, and quantity.
        # Then, we would check if it's still profitable.
        # If not, cancel, and check again, resetting our state to None.
        # Still need an order state machine
        # Setup logging!

        if self.state == BotState.STATE_WAITING_FOR_ARB:
            if profits[best_strat] > 0:
                self.strat = best_strat
                self.quantity = strat_volume[best_strat]
                self.maker_price = maker_prices[best_strat]
                self.taker_price = taker_prices[best_strat]
                logger.info(f'(M:{makers[best_strat]} T:{takers[best_strat]}) |PLACING ORDER| M{self.quantity}@{self.maker_price} T{self.quantity}@{self.taker_price}')

                # Make an exception for the taker/taker strat -- submits both orders, and stays in this state
                if best_strat == 4 or best_strat == 5:
                    logger.info(f'taker/taker ({makers[best_strat]}:{self.quantity}@{self.maker_price}/{takers[best_strat]}:{self.quantity}@{self.taker_price}) order')
                    return

                # Place order
                self.state = BotState.STATE_WAITING_FOR_MATCH
            else:
                logger.info(profits)
        elif self.state == BotState.STATE_WAITING_FOR_MATCH:
            # The following can cause order cancellation:
            # - Profit disappears
            # - Available quantity is reduced
            # - Someone beats our price (How to do regardless of bid / ask?)
            cancel_criteria = [ profits[self.strat] <= 0, strat_volume[self.strat] < self.quantity, better_maker_functions[self.strat](self.price, maker_prices[self.strat]) ]
            if reduce(lambda x,y: x or y, cancel_criteria):
                logger.info(f'(M:{makers[self.strat]} T:{takers[self.strat]}) |CANCELLING ORDER| profits <= 0: {cancel_criteria[0]} our_vol < avail_vol: {cancel_criteria[1]} better maker: {cancel_criteria[2]}')
                self.state = BotState.STATE_WAITING_FOR_ARB
        elif self.state == BotState.STATE_STOP:
            pass


    def transition(self, coinbase_data, kraken_data):
        self.handle_data(coinbase_data, kraken_data)
        '''
        if self.type == BotState.BID:
            self._handle_bid(coinbase_data, kraken_data)
        elif self.type == BotState.ASK:
            self._handle_ask(coinbase_data, kraken_data)
        '''

bid_bot = BotState(BotState.BID)
ask_bot = BotState(BotState.ASK)

bid_bot.balance['kraken'] = round(float(krak.get_account_balance()['result']['XREP']), Exchange.get_volume_precision('kraken', 'REP/USD'))
bid_bot.balance['coinbase'] = round(float(list(filter(lambda k: k['currency'] == 'REP', cb.get_accounts()))[0]['balance']), Exchange.get_volume_precision('coinbase', 'REP/USD'))
'''

# Technically should have a split of some sort
ask_bot.balance['kraken'] = bid_bot.balance['kraken']
ask_bot.balance['coinbase'] = bid_bot.balance['coinbase']
'''

print(f'bid bot kraken balance: {bid_bot.balance["kraken"]}')
print(f'bid bot coinbase balance: {bid_bot.balance["coinbase"]}')

print(f'ask bot kraken balance: {ask_bot.balance["kraken"]}')
print(f'ask bot coinbase balance: {ask_bot.balance["coinbase"]}')

def handle_arb(msg):
    global bid_bot
    global ask_bot
    global coinbase
    global kraken

    kraken_price_precision = 3
    kraken_vol_precision = 6
    coinbase_price_precision = 2
    coinbase_vol_precision = 6
    match = None

    #print(f'msg: {msg}')

    if 'exchange' in msg:
        if msg['exchange'] == 'coinbase':
            coinbase = msg
        elif msg['exchange'] == 'kraken':
            kraken = msg
    '''
    elif 'match' in msg:
        print(f'[match] maker: {msg["match"]["maker_order_id"]} taker: {msg["match"]["taker_order_id"]}')
        match = msg['match']['maker_order_id']
        match_size = msg['match']['size']
        #print(f'match: {match}')
    '''

    if coinbase != {} and kraken != {}:
        bid_bot.transition(coinbase, kraken)
        #ask_bot.transition(coinbase, kraken)
        return
        kraken_best_bid, kraken_best_bid_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), kraken['best_bid']))
        kraken_best_ask, kraken_best_ask_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), kraken['best_ask']))
        coinbase_best_bid, coinbase_best_bid_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), coinbase['best_bid']))
        coinbase_best_ask, coinbase_best_ask_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), coinbase['best_ask']))

        #print(f'[Coinbase] BBO: {coinbase_best_bid}/{coinbase_best_ask}')
        #print(f'[Kraken  ] BBO: {kraken_best_bid}/{kraken_best_ask}')

        mm_bid, taker_bid = (kraken_best_bid, coinbase_best_bid) if kraken_best_bid < coinbase_best_bid else (coinbase_best_bid, kraken_best_bid)
        mm_bid_fees, taker_bid_fees = (.0016, .0025) if kraken_best_bid < coinbase_best_bid else (.0015, .0026)
        mm_ask, taker_ask = (kraken_best_ask, coinbase_best_ask) if kraken_best_ask > coinbase_best_ask else (coinbase_best_ask, kraken_best_ask)
        mm_ask_fees, taker_ask_fees = (.0016, .0025) if kraken_best_ask > coinbase_best_ask else (.0015, .0026)

        bid_profit = taker_bid - mm_bid - (taker_bid*taker_bid_fees + mm_bid*mm_bid_fees)
        ask_profit = mm_ask - taker_ask - (mm_ask*mm_ask_fees + taker_ask*taker_ask_fees)

        print(f'MM bid: {mm_bid} taker bid: {taker_bid}')
        print(f'MM ask: {mm_ask} taker ask: {taker_ask}')

        if bid_profit > 0:
            print(f'Bid profit: {bid_profit}')
        if ask_profit > 0:
            print(f'Ask profit: {ask_profit}')

        return

        buy_price, buy_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), coinbase['best_bid']))
        our_price = round(buy_price + .01, coinbase_price_precision) # Change to CB precision
        #buy_price, buy_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), coinbase['best_ask']))
        cb_best_ask, _ = list(map(lambda k: round(float(k), simpleob.precision(k)), coinbase['best_ask']))


        sell_price, sell_vol = list(map(lambda k: round(float(k), simpleob.precision(k)), kraken['best_bid']))

        # Need to get the quantity that fits our budget
        # Quantity needs to take into account fees (make sure rounding is correct?)
        #quantity = min(buy_vol*buy_price*(1-.0015), sell_vol*buy_price*(1-.0015), bot.budget / (buy_price*(1-.0015)))
        quantity = min(buy_vol*our_price*(1-.0015), sell_vol*our_price*(1-.0015), bot.budget / (our_price*(1-.0015)))

        # Calculate differently depending on our state
        '''
        spread = sell_price*EUR_TO_USD - our_price
        #fees = EUR_TO_USD*sell_price*.0028 + buy_price*.0015
        fees = EUR_TO_USD*sell_price*.0028 + our_price*.0015
        profit = spread - fees
        #print(f'profit: {profit}')
        #return
        '''

        if bot.state == STATE_WAITING_FOR_ARB:
            spread = sell_price*EUR_TO_USD - our_price
            fees = EUR_TO_USD*sell_price*.0028 + our_price*.0015
            profit = spread - fees

            if profit > 0:
                bot.coinbase_bid = our_price
                bot.quantity = round(quantity, coinbase_vol_precision)
                # Limit order needs killed if its only partially filled (or just do fill or kill)
                print(f'Placing order {bot.quantity} @ {bot.coinbase_bid}: profit: {bot.quantity*profit} ({cb_best_ask})')
                # Is it possible for this to have an HTTP error?
                #bot.order = cb.limit_buy_order(COINBASE_ID, str(bot.coinbase_bid), str(bot.quantity), post_only=True)
                bot.order = cb.limit_buy_order(COINBASE_ID, str(bot.coinbase_bid), str(bot.quantity), post_only=True)
                print(f'order id: {bot.order["id"]}')
                bot.state = STATE_WAITING_FOR_MATCH
            else:
                print(f'profit: {profit} B: {buy_price} S: {EUR_TO_USD*sell_price} ({sell_price})')

        elif bot.state == STATE_WAITING_FOR_MATCH:
            # Current state of the spread
            spread = sell_price*EUR_TO_USD - bot.coinbase_bid 
            fees = sell_price*EUR_TO_USD*.0028 - bot.coinbase_bid*.0015
            profit = spread - fees

            if match and match == bot.order['id']:
                # Sell the returned quantity on kraken
                # Try and cancel the rest of the order? (If we can't, and it was filled, what to do?)
                print(f'Order matched!')
                remaining = bot.quantity - round(float(match_size), simpleob.precision(match_size))
                # Technically should be in kraken vol precision, but they're the same
                remaining = round(remaining, coinbase_vol_precision)
                if remaining > 0:
                    try:
                        print(f'Cancelling remainder of the order: {remaining}')
                        cb.cancel_order(bot.order['id'])
                    except requests.HTTPError as error:
                        print(f'Couldn\'t cancel remaining amount of order: {remaining}')

                print(f'Submitting short on kraken of size: {match_size}')
                # Submit kraken market short here
                krak.market_sell_order('REPEUR', match_size, leverage='2', validate=False)
                bot.state = STATE_TRANSFER_COINS
            elif profit < 0:
                try:
                    print(f'profit ({profit}) < 0, cancelling order.')
                    cb.cancel_order(bot.order['id'])
                    bot.state = STATE_WAITING_FOR_ARB
                except requests.HTTPError as error:
                    print(f'Couldn\'t cancel order: {error}')
                    bot.state = STATE_TRANSFER_COINS
            elif buy_price > bot.coinbase_bid:
                try:
                    print(f'price {bot.coinbase_bid} < current {buy_price}, cancelling order')
                    cb.cancel_order(bot.order['id'])
                    bot.state = STATE_WAITING_FOR_ARB
                except requests.HTTPError as error:
                    print(f'Couldn\'t cancel order: {error}')
                    bot.state = STATE_TRANSFER_COINS
            # If the price didn't move, we are on the same bid, which has a quantity + our bot quantity
            #elif round(quantity, coinbase_vol_precision) < bot.quantity:
            elif round(buy_vol, coinbase_vol_precision) < bot.quantity:
                try:
                    print(f'bot quantity {bot.quantity} > current {quantity}, cancelling order')
                    cb.cancel_order(bot.order['id'])
                    bot.state = STATE_WAITING_FOR_ARB
                except requests.HTTPError as error:
                    print(f'Couldn\'t cancel order: {error}')
                    bot.state = STATE_TRANSFER_COINS
            else:
                print(f'Waiting for match, profit: {profit} B: {buy_price} S: {sell_price} ({sell_price*EUR_TO_USD})')

        elif bot.state == STATE_TRANSFER_COINS:
            print('Just waiting to transfer coins.')

class EventFeed():
    def __init__(self):
        self.lock = None
        self.queue = None
        self.callback = None

    async def coinbase_feed(self):
        # Make sure sequence numbers are handled properly..
        async with websockets.connect('wss://ws-feed.pro.coinbase.com') as ws:
            await ws.send(json.dumps({
                'type': 'subscribe',
                'product_ids': [
                    COINBASE_ID
                ],
                'channels': [
                    'level2',
                    'matches'
                ]
            }))
            try:
                ob = pd.Series()
                last_trade_id = None
                while True:
                    msg = json.loads(await ws.recv())
                    msg_type = msg['type']
                    #print(msg_type)

                    # Use heartbeat sequence numbers and rest api
                    # for trade data to keep an up to date list of trades.
                    # This is necessary to update the program on whether a match occurred
                    if msg_type == 'snapshot':
                        '''
                        ob = {
                            'buy': { price: size for price, size in msg['bids'] },
                            'sell': { price: size for price, size in msg['asks'] }
                        }
                        '''
                        ob = simpleob.convert_coinbase_ob(msg)

                    if msg_type == 'l2update':
                        #print(msg['changes'])
                        #self.update_coinbase_ob(ob, msg)
                        simpleob.apply_ob_update(ob, simpleob.convert_coinbase_update(msg))
                        async with self.lock:
                            #best_bid = sorted(list(ob['buy'].keys()), key=lambda k: round(float(k),2), reverse=True)
                            #best_ask = sorted(list(ob['sell'].keys()), key=lambda k: round(float(k),2))
                            # I hope these quantities are correct
                            # This is why we need a uniform orderbook
                            '''
                            self.queue.put_nowait({
                                'exchange': 'coinbase',
                                'best_bid': best_bid[0],
                                'best_bid_vol': ob['buy'][best_bid[0]],
                                'best_ask_vol': ob['sell'][best_ask[0]],
                                'best_ask': best_ask[0]
                            })
                            '''
                            self.queue.put_nowait({
                                'exchange': 'coinbase',
                                'best_bid': simpleob.get_best_bid(ob),
                                'best_ask': simpleob.get_best_ask(ob)
                            })

                    '''
                    if msg_type == 'heartbeat':
                        #last_trade_id = msg['last_trade_id']
                        print(msg)
                        if last_trade_id != msg['last_trade_id']:
                            print(f'missed messages: ({last_trade_id}, {msg["last_trade_id"]}]')
                    '''

                    if msg_type == 'last_match':
                        last_trade_id = msg['trade_id']
                        #print(msg)

                    if msg_type == 'match':
                        if msg['trade_id'] - last_trade_id > 1:
                            print(f'[Dropped messages] last_trade_id: {last_trade_id} trade_id: {msg["trade_id"]}')
                        last_trade_id = msg['trade_id']

                        if not ob.empty:
                            async with self.lock:
                                self.queue.put_nowait({'exchange': 'coinbase',
                                    'match': msg,
                                    'best_bid': simpleob.get_best_bid(ob),
                                    'best_ask': simpleob.get_best_ask(ob)
                                })

            except asyncio.CancelledError:
                return

    def coinbase_taker_fee(self, price):
        return float(price) * .0025

    def kraken_taker_fee(self, price):
        return float(price) * .0026

    def update_coinbase_ob(self, ob, update):
        for side, price, size in update['changes']:
            if round(float(size), 2) == 0: # Maybe we should round to a degree..
                del ob[side][price]
            else:
                ob[side][price] = size

    def update_kraken_ob(self, ob, changes):
        for change in changes:
            if 'a' in change:
                for item in change['a']:
                    if item[-1] == 'r':
                        item = item[:-1]
                    level, quantity, timestamp = item
                    if float(level) < float(ob['as'][0][0]):
                        ob['as'].insert(0, [level, quantity, timestamp])
                    else:
                        for i, entry in enumerate(ob['as']):
                            if level in entry:
                                if float(quantity) != 0:
                                    ob['as'].pop(i)
                                    ob['as'].insert(i, entry)
                                else:
                                    ob['as'].pop(i)

            elif 'b' in change:
                for item in change['b']:
                    if item[-1] == 'r': 
                        item = item[:-1]
                    level, quantity, timestamp = item
                    if float(level) > float(ob['bs'][0][0]):
                        ob['bs'].insert(0, [level, quantity, timestamp])
                    else:
                        for i, entry in enumerate(ob['bs']):
                            if level in entry:
                                if float(quantity) != 0:
                                    ob['bs'].pop(i)
                                    ob['bs'].insert(i, entry)
                                else:
                                    ob['bs'].pop(i)

    async def kraken_feed(self):
        async with websockets.connect('wss://ws.kraken.com') as ws:
            # Eventually need to add code to handle token refreshing (shouldn't cause too many problems) 
            #token = krak.get_websocket_token()
            await ws.send(json.dumps({
                'event': 'subscribe',
                'pair': [KRAKEN_ID],
                'subscription': {
                    'name': 'book',
                },
            }))

            await ws.send(json.dumps({
                'event': 'subscribe',
                'pair': [KRAKEN_ID],
                'subscription': {
                    'name': 'trade',
                }
            }))

            try:
                ob = pd.Series()
                while True:
                    data = json.loads(await ws.recv())
                    #print(f'data: {data}')
                    if 'book-10' in data:
                        #if ob == {}:
                        if ob.empty:
                            ob = simpleob.convert_kraken_ob(data)
                        else:
                            #changes = list(filter(lambda k: type(k) == dict, data))
                            simpleob.apply_ob_update(ob, simpleob.convert_kraken_update(data), max_depth=10)
                            #self.update_kraken_ob(ob, changes)
                    elif 'trade' in data and not ob.empty:
                        async with self.lock:
                            self.queue.put_nowait({'exchange': 'kraken',
                                'match': data[1],
                                'best_bid': simpleob.get_best_bid(ob),
                                'best_ask': simpleob.get_best_ask(ob)}) # Just a filler message for now
                        continue
                    elif data['event'] == 'heartbeat': continue
                    elif data['event'] == 'systemStatus': continue
                    #elif data['event'] == 'subscriptionStatus':
                    #    print(data)

                    if not ob.empty:
                        async with self.lock:
                            '''
                            self.queue.put_nowait({'exchange': 'kraken',
                                'best_bid': ob['bs'][0][0],
                                'best_bid_vol': ob['bs'][0][1],
                                'best_ask_vol': ob['as'][0][1],
                                'best_ask': ob['as'][0][0]})
                            '''
                            self.queue.put_nowait({'exchange': 'kraken',
                                'best_bid': simpleob.get_best_bid(ob),
                                'best_ask': simpleob.get_best_ask(ob)})
            except asyncio.CancelledError:
                return

    def set_callback(self, callback):
        self.callback = callback

    # [ "Buy on kraken, sell on CB", "Buy on CB, sell on kraken" ]
    def profit(self, coinbase, kraken):
        return [ 
            float(kraken['best_bid']) - float(coinbase['best_ask']) -\
                    self.kraken_taker_fee(float(kraken['best_bid'])) -\
                    self.coinbase_taker_fee(coinbase['best_ask']),
            float(coinbase['best_bid']) - float(kraken['best_ask']) -\
                    self.kraken_taker_fee(float(kraken['best_ask'])) -\
                    self.coinbase_taker_fee(coinbase['best_bid'])
        ]

    async def manage_queue(self):
        try:

            while True:
                #async with self.lock:
                msg = await self.queue.get()
                if self.callback:
                    self.callback(msg)
                
        except asyncio.CancelledError:
            return

    async def _start(self):
        self.lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        await asyncio.gather(self.coinbase_feed(), self.kraken_feed(),
                self.manage_queue())

    def run(self):
        asyncio.run(self._start())

    def coinbase_submit_order(self):
        pass

    def kraken_submit_order(self):
        pass

feed = EventFeed()
feed.set_callback(handle_arb)
#order = cb.limit_buy_order('BTC-USD', '10.15', '2.46305419', post_only=True)
asyncio.run(feed._start())
#asyncio.run(feed.kraken_feed(None))
#asyncio.run(feed.coinbase_feed(None))
