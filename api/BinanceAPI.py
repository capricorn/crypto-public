import asyncio
import json
import time
import hmac
from decimal import Decimal

import websockets
import requests
import pandas as pd

from api import ob as simpleob
from api import common
from api import exchange
from api import wrappers

class BinanceAuth(requests.auth.AuthBase):
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def sign(self, msg):
        sig = hmac.new(self.api_secret.encode(), msg.encode(), 'sha256')
        return sig.hexdigest()

    def __call__(self, request):
        request.headers.update({'X-MBX-APIKEY': self.api_key})
        if request.method == 'POST' or request.method == 'DELETE':
            sig = self.sign(request.body) 
            request.body += f'&signature={sig}'
        elif request.method == 'GET':
            data = request.path_url[request.path_url.index('?')+1:]
            sig = self.sign(data) 
            request.prepare_url(request.url, {'signature': sig})

        return request

class BinanceAPI():
    ENDPOINT = 'https://api.binance.us'
    EXCHANGE = 'binance_us'

    def __init__(self, auth_data):
        self.api_key = auth_data['binance_us']['api_key']
        self.api_secret = auth_data['binance_us']['api_secret']

    def get_wallet_balance(self, asset):
        wallet = list(filter(lambda k: k['asset'] == asset, self.get_account_information()['balances']))[0]
        return common.Wallet(asset=asset, balance=Decimal(wallet['free']))

    def get_account_information(self):
        params = {
            'timestamp': int(time.time()*1000)
        }

        resp = requests.get(f'{BinanceAPI.ENDPOINT}/api/v3/account', params=params, auth=BinanceAuth(self.api_key, self.api_secret))
        resp.raise_for_status()
        return resp.json()

    def get_fees(self):
        info = self.get_account_information()
        # Possible that this should be 'makerCommission' and 'takerCommission'
        return common.Fees(Decimal(info['buyerCommission']), Decimal(info['sellerCommission']))

    def _order(self, pair, side, ordertype, quantity, **kwargs):
        # Just add the timestamp here

        if 'post_only' in kwargs:
            ordertype = 'LIMIT_MAKER'

        order = {
            'symbol': self.convert_pair(pair).upper(),
            'side': side,
            'type': ordertype,
            'quantity': quantity,
            'newOrderRespType': 'FULL',
            'timestamp': int(time.time()*1000),
            'recvWindow': 10000
        }

        if 'IOC' in kwargs:
            order['timeInForce'] = 'IOC'
        if 'FOK' in kwargs:
            order['timeInForce'] = 'FOK'

        if ordertype == 'LIMIT':
            order['price'] = kwargs['price']
            if 'timeInForce' not in order:
                order['timeInForce'] = 'GTC'
            #order['timeInForce'] = time_in_force
        elif ordertype == 'LIMIT_MAKER':
            order['price'] = kwargs['price']

        '''
        if ordertype == 'LIMIT' or ordertype == 'LIMIT_MAKER':
            try:
                order['price'] = kwargs['price']
                #order['timeInForce'] = time_in_force
            except KeyError as e:
                raise ValueError(f'Missing required parameter for limit order: {e.args[0]}')
        '''
        
        resp = requests.post(f'{BinanceAPI.ENDPOINT}/api/v3/order', data=order, auth=BinanceAuth(self.api_key, self.api_secret))
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise common.ExchangeException('binance_us', e.response.text)
        resp = resp.json()
        return common.Order(resp['orderId'], resp['type'].lower(), resp['side'].lower(), Decimal(resp['origQty']), price=Decimal(resp['price']))

    def cancel_order(self, order_id, **kwargs):
        order = {
            'symbol': self.convert_pair(kwargs['pair']).upper(),
            'orderId': order_id,
            'timestamp': int(time.time()*1000),
            'recvWindow': 10000
        }

        resp = requests.delete(f'{BinanceAPI.ENDPOINT}/api/v3/order', data=order, auth=BinanceAuth(self.api_key, self.api_secret))
        resp.raise_for_status()
        return resp.json()

    def limit_buy_order(self, pair, price, quantity, **kwargs):
        return self._order(pair, 'BUY', 'LIMIT', quantity, price=price, **kwargs)

    def limit_sell_order(self, pair, price, quantity, **kwargs):
        return self._order(pair, 'SELL', 'LIMIT', quantity, price=price, **kwargs)

    def test_trade(self, pair):
        # Does timestamp need to match the auth timestamp..?
        order = {
            'symbol': 'ETHUSD',
            'side': 'SELL',
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'quantity': '0.1',
            'price': '200.0',
            'newOrderRespType': 'FULL',
            'recvWindow': 5000,
            'timestamp': int(time.time()*1000) # Just move into auth
        }
        resp = requests.post(f'{BinanceAPI.ENDPOINT}/api/v3/order/test', data=order, auth=BinanceAuth(self.api_key, self.api_secret))
        resp.raise_for_status()
        return resp.json()

    def get_open_orders(self):
        data = {
            'symbol': 'BTCUSD',
            'timestamp': int(time.time()*1000)
        }

        resp = requests.get(f'{BinanceAPI.ENDPOINT}/api/v3/openOrders', params=data, auth=BinanceAuth(self.api_key, self.api_secret))
        resp.raise_for_status()
        return resp.json()

    def get_products(self):
        resp = requests.get(f'{BinanceAPI.ENDPOINT}/api/v1/exchangeInfo')
        resp.raise_for_status()
        return _parse_exchange_info(resp.json())

    def get_server_time(self):
        return requests.get(f'{BinanceAPI.ENDPOINT}/api/v1/time').json()

    def get_feed_key(self):
        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        resp = requests.post(f'{BinanceAPI.ENDPOINT}/api/v1/userDataStream', headers=headers)
        resp.raise_for_status()
        return resp.json()['listenKey']

    def renew_feed_key(self, key):
        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        params = {
            'listenKey': key
        }

        resp = requests.put(f'{BinanceAPI.ENDPOINT}/api/v1/userDataStream', headers=headers, params=params)
        resp.raise_for_status()

    def delete_feed_key(self, key):
        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        params = {
            'listenKey': key
        }

        resp = requests.delete(f'{BinanceAPI.ENDPOINT}/api/v1/userDataStream', headers=headers, params=params)
        resp.raise_for_status()

    def get_orderbook(self, pair, limit=None):
        params = { 
            'symbol': self.convert_pair(pair).upper()
        }

        if limit:
            params['limit'] = limit

        ob = requests.get(f'{BinanceAPI.ENDPOINT}/api/v1/depth', params=params).json()
        return _standardize_orderbook(ob)

    @staticmethod
    def convert_pair(pair):
        return pair.lower().replace('/', '')

class BinanceWebsocket():
    WEBSOCKET = 'wss://stream.binance.us:9443'
    EXCHANGE = 'binance_us'

    @staticmethod
    async def connect():
        return BinanceWebsocket()

    async def _queue_events(self):
        while True:
            try:
                await self.subscribe_event.wait()
                async for event in self.ws:
                    if self.iterating:
                        data = json.loads(event)
                        data = data['data'] if 'stream' in data else data
                        subevents = self.parse(data)
                        if type(subevents) == list:
                            for subevent in self.parse(data):
                                self.queue.put_nowait(subevent)
                        else:
                            self.queue.put_nowait(subevents)
                        #self.queue.put_nowait(wrappers.BinanceWebsocketWrapper.parse(data))
                        #await asyncio.sleep(0)
            except websockets.exceptions.ConnectionClosed:
                print('binance connection closed')
            except asyncio.CancelledError:
                raise

    async def subscribe_orderbook_feed(self, pair):
        await self.subscribe(f'{BinanceAPI.convert_pair(pair)}@depth@100ms')

    async def subscribe_user_feed(self, auth_data, **kwargs):
        api = BinanceAPI(auth_data)
        channel = api.get_feed_key()
        await self.subscribe(channel)

    async def subscribe(self, channel):
        self.channels.add(channel)
        streams = list(self.channels)

        if len(self.channels) == 1:
            stream = f'{BinanceWebsocket.WEBSOCKET}/ws/{streams[0]}'
        elif len(self.channels) > 1:
            stream = f'{BinanceWebsocket.WEBSOCKET}/stream?streams={"/".join(streams)}'

        if self.ws:
            self.subscribe_event.clear()
            await self.ws.close()

        self.ws = await websockets.connect(stream)
        self.subscribe_event.set()

    def __init__(self):
        self.orders = {}
        self.ws = None
        self.queue_event_task = asyncio.create_task(self._queue_events())
        self.queue = asyncio.Queue()
        self.channels = set()
        self.max_queue_size = 10
        self.subscribe_event = asyncio.Event()
        self.iterating = True

    def __aiter__(self):
        # In the future, stop when iterating ends
        #self.iterating = True
        return self

    def __anext__(self):
        return self.queue.get()

    def parse(self, data):
        parsers = {
            'depthUpdate': self.parse_orderbook_update,
            'executionReport': self.parse_order,
            'outboundAccountInfo': lambda e: common.Event('account_info', 'binance'),
            'outboundAccountPosition': lambda e: common.Event('account_position', 'binance')
        }

        return parsers[data['e']](data)

    def parse_orderbook_update(self, data):
        updates = []

        updates.extend(list(map(lambda bid: ['bids', *bid], data['b'])))
        updates.extend(list(map(lambda ask: ['asks', *ask], data['a'])))

        return common.OrderbookEvent('binance_us', updates, data['U'])

    def parse_order(self, data):
        # Would be nice to parse data into a named tuple
        #print(f'order: {data}')
        #print(f'order: {data["x"]}, {data["X"]}, {data["i"]}, {data["p"]}, {data["q"]}, {data["l"]}, {data["z"]}, {data["L"]}, {data["t"]}')
        print(f'Binance: {data["x"]}, {data["X"]}')
        if data['x'] == 'NEW':
            #self.orders[data['i']] = (Decimal(data['p']), Decimal(data['q']))
            return common.OrderOpenEvent('binance_us', data['i'], data['p'], data['q'])
            '''
            if data['o'] == 'LIMIT':
                return common.OrderOpenEvent('binance', data['i'], data['p'], data['q'])
            elif data['o'] == 'MARKET':
                return common.OrderOpenEvent('binance', data['i'], data['p'], data['q'])
            elif data['o'] == 'LIMIT_MARKET':
                # TODO 
                pass
            '''
        elif data['x'] == 'CANCELED' or data['x'] == 'REJECTED' or data['x'] == 'EXPIRED':
            #del self.orders[data['i']]
            return common.OrderDoneEvent('binance', data['i'], 'cancelled', side=data['S'].lower(), price=Decimal(data['p']), quantity=Decimal(data['q']))
        elif data['x'] == 'TRADE':
            # Need to return a MatchEvent, and then DoneEvent, if the order filled (probably need to integrate into binance websocket to track this)
            # We need to output an order of the partial amount filled
            #return common.OrderMatchEvent('binance', data['i'], data['p'], float(data['z']) - float(data['l']))
            events = []

            #events.append(common.OrderMatchEvent('binance_us', data['i'], data['p'], data['q']))
            events.append(common.OrderMatchEvent('binance_us', data['i'], data['L'], data['l'], side=data['S'].lower()))
            if data['X'] == 'FILLED':   # May also have to deal with cancels from IOC
                events.append(common.OrderDoneEvent('binance_us', data['i'], 'filled', side=data['S'].lower(), price=Decimal(data['p']), quantity=Decimal(data['q'])))
            '''
            print(f"order exists: {data['i'] in self.orders}")
            #print(f'{data["i"]} ({self.orders[data["i"]]}) cumulative filled: {data["z"]}')
            if data['i'] in self.orders and Decimal(data['z']) == self.orders[data['i']][1]:
                events.append(common.OrderDoneEvent('binance_us', data['i'], 'filled'))
                del self.orders[data['i']]
            '''

            return events

def _parse_exchange_info(info):
    products = []
    for prod in info['symbols']:
        # Eventually add in tick size
        base_increment = [ asset_filter for asset_filter in prod['filters'] if asset_filter['filterType'] == 'LOT_SIZE' ][0]['stepSize']
        base_increment = base_increment[:base_increment.index('1')+1]

        quote_increment = [ asset_filter for asset_filter in prod['filters'] if asset_filter['filterType'] == 'PRICE_FILTER' ][0]['tickSize']
        quote_increment = quote_increment[:quote_increment.index('1')+1]

        min_notional = [ asset_filter for asset_filter in prod['filters'] if asset_filter['filterType'] == 'MIN_NOTIONAL' ][0]['minNotional']
        products.append(common.ProductInfo(prod['symbol'].lower(), prod['baseAsset'], prod['quoteAsset'], Decimal(base_increment), Decimal(quote_increment), Decimal(min_notional)))

    return products
    #return [ common.ProductInfo(prod['symbol'].lower(), prod['baseAsset'], prod['quoteAsset'], Decimal('10') ** -prod['baseAssetPrecision'], Decimal('10') ** -prod['quoteCommissionPrecision']) for prod in info['symbols'] ]

def _standardize_orderbook(ob):
    return pd.Series({
        'bids': pd.DataFrame(ob['bids'], columns=['price', 'quantity']),
        'asks': pd.DataFrame(ob['asks'], columns=['price', 'quantity']),
        'sequence': ob['lastUpdateId']
    })
    return ob

def _standardize_ob_update(update):
    updates = []

    updates.extend(list(map(lambda bid: ['bids', *bid], update['b'])))
    updates.extend(list(map(lambda ask: ['asks', *ask], update['a'])))

    return updates
