import hmac
import base64
import time
import hashlib
import json
import uuid
import asyncio
import itertools
import heapq
import logging

from decimal import Decimal
from requests.auth import AuthBase
import requests
import websockets

from api import ob as simpleob
from api import common
from api import wrappers

log = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(asctime)s %(module)s %(levelname)s %(funcName)s %(message)s'))
log.addHandler(_handler)

class PriceLevel():
    def __init__(self, price, order_id, quantity, side):
        self.price = price
        self.quantity = quantity
        self.orders = {}
        self.orders[order_id] = quantity
        self.side = side

    def __init__(self, price, orders, side):
        self.price = price
        self.side = side
        self.orders = orders
        self.quantity = sum(self.orders.values())

    def reduce(self, order_id, quantity):
        ''' Subtracts quantity from order_id on the books, and deletes if 0. Used for match messages. '''
        self.orders[order_id] -= quantity
        self.quantity -= quantity

        if self.quantity == 0:
            del self.orders[order_id]

    def add(self, order_id, quantity):
        ''' Adds order_id to orders with given quantity. Used for open messages. '''
        self.orders[order_id] = quantity
        self.quantity += quantity

    def dead(self):
        ''' Return true if this price level is dead, and should be removed '''
        return self.orders == {}

    # Necessary for heap tuple comparison
    # Maybe not? 
    def __getitem__(self, key):
        price = -price if side == 'bid' else price
        return (price, self.quantity)[key]

    def __lt__(self, other):
        return self.price > other.price if self.side == 'bid' else self.price < other.price

# Coinbase Non-aggregated OB for full stream
# Updates on match and open messages
class CoinbaseFullOrderbook():
    def __init__(self, snapshot):
        # Orderbook is indexed by side, and consists of a hashtable which stores
        # order ids, and (price, quantity, time?) tuples
        # best_bid() and best_ask() aggregates the book
        # How can we construct this to use a heap..?
        # Could index by price level, which then contains a hashtable of (order_id -> quantity)
        # side -> level -> order_id -> quantity
        # May need custom level objects for the heap?

        # Create heap here, using price level. Easiest way to convert non-aggregated snapshot?
        # Maybe this is a good case for using groups..? (Sort by price first)
        # Basically, create a list of price levels from groups, and heapify.
        # Entry is (price, quantity, order_id)
        self.orderbook = {}
        bids = sorted([ (entry.price, entry.quantity, entry.order_id) for entry in snapshot['bids'].itertuples() ], key=lambda entry: entry[0], reverse=True)
        aggregated_bids = { Decimal(price_level): { order_id: Decimal(q) for _, q, order_id in list(bid_group) } for price_level, bid_group in itertools.groupby(bids, key=lambda key: key[0]) }

        self.orderbook['buy'] = [ PriceLevel(price, orders, 'bid') for price, orders in aggregated_bids.items() ]
        heapq.heapify(self.orderbook['buy'])

        asks = sorted([ (entry.price, entry.quantity, entry.order_id) for entry in snapshot['asks'].itertuples() ], key=lambda entry: entry[0], reverse=True)
        aggregated_asks = { Decimal(price_level): { order_id: Decimal(q) for _, q, order_id in list(ask_group) } for price_level, ask_group in itertools.groupby(asks, key=lambda key: key[0]) }

        self.orderbook['sell'] = [ PriceLevel(price, orders, 'ask') for price, orders in aggregated_asks.items() ]
        heapq.heapify(self.orderbook['sell'])

    def best_bid(self):
        level = self.orderbook['buy'][0]
        return (level.price, level.quantity)

    def best_ask(self):
        level = self.orderbook['sell'][0]
        return (level.price, level.quantity)

    def update(self, event):
        if event.event_type == 'order_open':
            # Update format: [ [side, price, quantity] ] 0 means delete
            #ob.update([['bids' if event.side == 'buy' else 'asks', D(event.price), D(event.quantity)]])
            # Worried about indexing by price, since strange prices can be set in limit orders
            self.orderbook[event.side][event.price].add(event.order_id, event.quantity)
        elif event.event_type == 'order_match':
            #ob.update([['bids' if event.side == 'buy' else 'asks', D(event.price), D(event.size)]])
            self.orderbook[event.side][event.price].reduce(event.order_id, event.quantity)

class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    # Don't we want the timestamp in milliseconds..?
    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256)
        #signature_b64 = signature.digest().encode('base64').rstrip('\n')
        signature_b64 = base64.b64encode(signature.digest())

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

class CoinbaseAPI():
    WEBSOCKET = 'wss://ws-feed.pro.coinbase.com'
    EXCHANGE = 'coinbase'

    def __init__(self, auth_data):
        self.auth = CoinbaseExchangeAuth(auth_data['coinbase']['api_key'], auth_data['coinbase']['api_secret'],
                auth_data['coinbase']['passphrase'])
        self.sandbox_auth = CoinbaseExchangeAuth(auth_data['coinbase']['sandbox_api_key'], 
                auth_data['coinbase']['sandbox_api_secret'],
                auth_data['coinbase']['sandbox_passphrase'])
        self.auth_data = auth_data

    @staticmethod
    def convert_pair(pair):
        return pair.replace('/', '-')

    def _get_api_endpoint(self, sandbox):
        return 'https://api-public.sandbox.pro.coinbase.com' if sandbox else 'https://api.pro.coinbase.com'
    
    def _get_auth(self, sandbox):
        return self.sandbox_auth if sandbox else self.auth

    def _sign_message(self, timestamp, method, req_path, body=''):
        return base64.b64encode(hmac.digest(base64.b64decode(self.api_secret), 'f{timestamp}{method}{req_path}{body}'.encode(),
            'sha256')).decode()

    def _auth_post(self, req_path, params={}, data={}, sandbox=False):
        resp = requests.post(f'{self._get_api_endpoint(sandbox)}{req_path}', auth=self._get_auth(sandbox), params=params, 
                data=json.dumps(data))

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 400:
                raise common.OrderException(e.response.reason)
            else:
                raise e

        return resp.json()

    def _auth_get(self, req_path, params={}, sandbox=False):
        resp = requests.get(f'{self._get_api_endpoint(sandbox)}{req_path}', auth=self._get_auth(sandbox), params=params)
        resp.raise_for_status()
        return resp.json()

    def get_fees(self, sandbox=False):
        resp = self._auth_get('/fees', sandbox=sandbox)

        return common.Fees(Decimal(resp['maker_fee_rate']), Decimal(resp['taker_fee_rate']))

    def get_trailing_volume(self, sandbox=False):
        return self._auth_get('/users/self/trailing-volume', sandbox=sandbox)

    def get_orderbook(self, pair, sandbox=False):
        params = {
            'level': 3
        }

        ob = self._auth_get(f'/products/{CoinbaseAPI.convert_pair(pair)}/book', params=params, sandbox=sandbox)
        return (ob['sequence'], wrappers.CoinbaseAPIWrapper.parse_get_orderbook(ob))

    def get_open_orders(self, status=['open','pending','active'], product_id=None, sandbox=False):
        params = {
            'status': status
        }
        if product_id: 
            params['product_id'] = product_id

        return self._auth_get('/orders', params=params, sandbox=sandbox)

    def _market_order(self, side, product_id, size, sandbox=False):
        order = {
            'type': 'market',
            'side': side,
            'product_id': product_id,
            'size': size
        }
        
        resp = self._auth_post('/orders', data=order, sandbox=sandbox)
        return common.Order(resp['id'], 'market', side, size, resp['executed_value'])

    def market_buy_order(self, product_id, size, sandbox=False):
        return self._market_order('buy', product_id, size, sandbox=sandbox)

    def market_sell_order(self, product_id, size, sandbox=False):
        return self._market_order('sell', product_id, size, sandbox=sandbox)

    def _limit_order(self, side, product_id, price, size, post_only=False, sandbox=False):
        params = {
            #'client_oid': str(uuid.uuid4()),
            'type': 'limit',
            'side': side,
            'product_id': CoinbaseAPI.convert_pair(product_id),
            'price': f'{price}',
            'size': f'{size}',
            'post_only': 'true', # this needs to be based off the variable!
            'time_in_force': 'GTC'
        }
        log.debug(f'pair: {product_id} price: {params["price"]} quantity: {params["size"]}')
        resp = self._auth_post('/orders', data=params, sandbox=sandbox)
        return common.Order(resp['id'], 'limit', side, size, price)

    def limit_buy_order(self, product_id, price, size, post_only=False, sandbox=False):
        return self._limit_order('buy', product_id, price, size, post_only=post_only, sandbox=sandbox)

    def limit_sell_order(self, product_id, price, size, post_only=False, sandbox=False):
        return self._limit_order('sell', product_id, price, size, post_only=post_only, sandbox=sandbox)

    # Eventually change API to just return true or false if it successfully cancelled
    def cancel_order(self, order_id, sandbox=False):
        resp = requests.delete(f'{self._get_api_endpoint(sandbox)}/orders/{order_id}', auth=self._get_auth(sandbox))
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code == 400:
                raise common.OrderException(e.response.reason)
            else:
                raise e
        return resp.json()

    def get_payment_methods(self, sandbox=False):
        return self._auth_get('/payment-methods', sandbox=sandbox)

    def get_accounts(self, sandbox=False):
        return self._auth_get('/accounts', sandbox=sandbox)

    def get_products(self, sandbox=False):
        resp = requests.get(f'{self._get_api_endpoint(sandbox)}/products')
        resp.raise_for_status()
        return self._parse_products(resp.json())

    def get_currencies(self, sandbox=False):
        resp = requests.get(f'{self._get_api_endpoint(sandbox)}/currencies')
        resp.raise_for_status()
        return [ common.CurrencyInfo(c['name'], 
            c['id'], 
            Decimal(c['min_size']), 
            c['details']['min_withdrawal_amount'] if 'min_withdrawal_amount' in c['details'] else Decimal(0), 
            Decimal(c['max_precision'])) for c in resp.json() ]

    def get_wallet_balance(self, asset):
        wallet = list(filter(lambda k: k['currency'] == asset, self.get_accounts()))[0]

        return common.Wallet(asset=asset, balance=Decimal(wallet['balance']))

    def withdraw_crypto(self, currency, amount, dest_address, sandbox=False):
        data = {
            'amount': amount,
            'currency': currency,
            'crypto_address': dest_address
        }
        return self._auth_post('/withdrawals/crypto', data=data, sandbox=sandbox)

    def _parse_products(self, products):
        # We need to obtain max_precision from currencies, and set it as base_increment  
        #currencies = self.get_currencies()
        res = []
        for prod in products:
            #currency = list(filter(lambda k: k.id == prod['base_currency'], currencies))[0]
            res.append(common.ProductInfo(prod['id'], prod['base_currency'], prod['quote_currency'], Decimal(prod['base_increment']).normalize(), 
                Decimal(prod['quote_increment']).normalize(), Decimal(prod['min_market_funds'])))

        #return [ common.ProductInfo(prod['id'], prod['base_currency'], prod['quote_currency'], Decimal(prod['base_min_size']), Decimal(prod['quote_increment'])) for prod in products ]
        return res

    @staticmethod
    def _parse_match_event(event):
        return common.MatchEvent('coinbase', event['price'], event['size'], event['side'])

    @staticmethod
    def _parse_orderbook_event(event):
        if event['type'] == 'snapshot':
            return common.OrderbookEvent('coinbase', simpleob.convert_coinbase_ob(event))

        return common.OrderbookEvent('coinbase', simpleob.convert_coinbase_update(event))

    @staticmethod
    def parse_websocket_event(event):
        data = event['data']
        if data['type'] == 'match':
            return CoinbaseAPI._parse_match_event(data)
        elif data['type'] == 'snapshot' or data['type'] == 'l2update':
            return CoinbaseAPI._parse_orderbook_event(data)

        return None

class CoinbaseWebsocket():
    WEBSOCKET = 'wss://ws-feed.pro.coinbase.com'
    EXCHANGE = 'coinbase'

    @staticmethod
    async def connect():
        ws = await websockets.connect(CoinbaseWebsocket.WEBSOCKET)
        return CoinbaseWebsocket(ws)

    async def subscribe(self, pair, *channels):
        await self.ws.send(json.dumps({
            'type': 'subscribe',
            'product_ids': [
                CoinbaseAPI.convert_pair(pair)
            ],
            'channels': [
                *channels
            ]
        }))

    async def subscribe_orderbook_feed(self, pair):
        await self.subscribe(CoinbaseAPI.convert_pair(pair), 'level2')

    # Make pair an input, only sub to user channel
    async def subscribe_user_feed(self, auth_data, **kwargs):
        if not 'pair' in kwargs:
            raise ValueError('"pair" keyword argument required.')

        timestamp = str(int(time.time()))
        message = timestamp + 'GET' + '/users/self/verify'
        hmac_key = base64.b64decode(auth_data['coinbase']['api_secret'])
        signature = hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        await self.ws.send(json.dumps({
            'type': 'subscribe',
            "product_ids": [
                CoinbaseAPI.convert_pair(kwargs['pair'])
            ],
            'channels': ['user'],
            'signature': signature_b64,
            'key': auth_data['coinbase']['api_key'],
            'passphrase': auth_data['coinbase']['passphrase'],
            'timestamp': timestamp
        }))

    async def _queue_events(self):
        try:
            async for event in self.ws:
                if self.iterate:
                    self.queue.put_nowait(wrappers.CoinbaseWebsocketWrapper.parse(json.loads(event)))
                    #await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise

    def __init__(self, ws):
        #self.ws = await websockets.connect(WEBSOCKET)
        self.ws = ws
        self.queue = asyncio.Queue()
        self.queue_event_task = asyncio.create_task(self._queue_events())
        self.iterate = True

    def __aiter__(self):
        #self.iterate = True
        return self

    async def __anext__(self):
        return await self.queue.get()
