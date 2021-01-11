import asyncio
import json
import datetime

import requests
import websockets
import CoinbaseAPI as cbapi
import KrakenAPI as kapi
import ob as simpleob
import pandas as pd

# 43bp.py

KRAKEN_ID = 'REP/EUR'
COINBASE_ID = 'REP-USD'
#KRAKEN_ID = 'XRP/EUR'
#COINBASE_ID = 'XRP-USD'
#KRAKEN_ID = 'ETH/EUR'
#COINBASE_ID = 'ETH-USD'
#KRAKEN_ID = 'BTC/EUR'
#COINBASE_ID = 'BTC-USD'

coinbase = {}
kraken = {}
STATE_WAITING_FOR_ARB = 0
STATE_WAITING_FOR_MATCH = 1
STATE_TRANSFER_COINS = 2

sandbox = False
API_KEY = ''
API_SECRET = ''
API_PASSPHRASE = ''
KRAKEN_API_KEY = ''
KRAKEN_API_PRIV_KEY = ''

# nonce = current timestamp in hundreths of seconds
# place / cancel orders don't affect call rate limit (recommends 1/sec)
krak = kapi.KrakenAPI(KRAKEN_API_KEY, KRAKEN_API_PRIV_KEY)

#cb = cbapi.CoinbaseAPI(API_KEY, API_SECRET, API_PASSPHRASE, sandbox=True)
cb = cbapi.CoinbaseAPI(API_KEY, API_SECRET, API_PASSPHRASE, sandbox=sandbox)
EUR_TO_USD = 1.11

class BotState():
    def __init__(self):
        self.state = STATE_WAITING_FOR_ARB
        # Max amount to spend on coinbase
        self.budget = 25
        # Current coinbase bid
        self.coinbase_bid = 0
        # Current quantity to buy / short
        self.quantity = 0
        # Current ask price on kraken
        self.kraken_ask = 0
        # Last submitted order on coinbase
        self.coinbase_order = None

bot = BotState()

def handle_arb(msg):
    # I'd like to bake this state into a function
    global bot
    global coinbase
    global kraken

    kraken_price_precision = 3
    kraken_vol_precision = 6
    coinbase_price_precision = 2
    coinbase_vol_precision = 6
    match = None

    if 'exchange' in msg:
        if msg['exchange'] == 'coinbase':
            coinbase = msg
        elif msg['exchange'] == 'kraken':
            kraken = msg
    elif 'match' in msg:
        print(f'[match] maker: {msg["match"]["maker_order_id"]} taker: {msg["match"]["taker_order_id"]}')
        match = msg['match']['maker_order_id']
        match_size = msg['match']['size']
        #print(f'match: {match}')

    if coinbase != {} and kraken != {}:
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
                        async with self.lock:
                            self.queue.put_nowait({
                                'match': msg
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
            await ws.send(json.dumps({
                'event': 'subscribe',
                'pair': [KRAKEN_ID],
                'subscription': {
                    'name': 'book',
                }
            }))
            try:
                #ob = {}
                ob = pd.Series()
                while True:
                    data = json.loads(await ws.recv())
                    if 'book-10' in data:
                        #if ob == {}:
                        if ob.empty:
                            ob = simpleob.convert_kraken_ob(data)
                        else:
                            #changes = list(filter(lambda k: type(k) == dict, data))
                            simpleob.apply_ob_update(ob, simpleob.convert_kraken_update(data), max_depth=10)
                            #self.update_kraken_ob(ob, changes)
                    elif data['event'] == 'heartbeat': continue
                    elif data['event'] == 'systemStatus': continue

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
