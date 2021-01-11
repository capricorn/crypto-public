import asyncio
import json
import time
import hmac
import re
from urllib.parse import urlencode
import logging
from decimal import Decimal

import requests
import websockets
import pandas as pd

from api import common
from api import wrappers

POLONIEX_PAIRS = {
    177: 'BTC_ARDR',
    253: 'BTC_ATOM',
    210: 'BTC_BAT',
    189: 'BTC_BCH',
    236: 'BTC_BCHABC',
    238: 'BTC_BCHSV',
    7: 'BTC_BCN',
    232: 'BTC_BNT',
    14: 'BTC_BTS',
    15: 'BTC_BURST',
    194: 'BTC_CVC',
    24: 'BTC_DASH',
    162: 'BTC_DCR',
    25: 'BTC_DGB',
    27: 'BTC_DOGE',
    201: 'BTC_EOS',
    171: 'BTC_ETC',
    148: 'BTC_ETH',
    155: 'BTC_FCT',
    246: 'BTC_FOAM',
    198: 'BTC_GAS',
    185: 'BTC_GNT',
    251: 'BTC_GRIN',
    43: 'BTC_HUC',
    207: 'BTC_KNC',
    213: 'BTC_LOOM',
    250: 'BTC_LPT',
    163: 'BTC_LSK',
    50: 'BTC_LTC',
    51: 'BTC_MAID',
    229: 'BTC_MANA',
    64: 'BTC_NMC',
    248: 'BTC_NMR',
    69: 'BTC_NXT',
    196: 'BTC_OMG',
    58: 'BTC_OMNI',
    249: 'BTC_POLY',
    75: 'BTC_PPC',
    221: 'BTC_QTUM',
    174: 'BTC_REP',
    170: 'BTC_SBD',
    150: 'BTC_SC',
    204: 'BTC_SNT',
    200: 'BTC_STORJ',
    89: 'BTC_STR',
    182: 'BTC_STRAT',
    92: 'BTC_SYS',
    97: 'BTC_VIA',
    100: 'BTC_VTC',
    108: 'BTC_XCP',
    112: 'BTC_XEM',
    114: 'BTC_XMR',
    116: 'BTC_XPM',
    117: 'BTC_XRP',
    178: 'BTC_ZEC',
    192: 'BTC_ZRX',
    211: 'ETH_BAT',
    190: 'ETH_BCH',
    202: 'ETH_EOS',
    172: 'ETH_ETC',
    176: 'ETH_REP',
    179: 'ETH_ZEC',
    193: 'ETH_ZRX',
    254: 'USDC_ATOM',
    235: 'USDC_BCH',
    237: 'USDC_BCHABC',
    239: 'USDC_BCHSV',
    224: 'USDC_BTC',
    243: 'USDC_DOGE',
    225: 'USDC_ETH',
    252: 'USDC_GRIN',
    244: 'USDC_LTC',
    242: 'USDC_STR',
    226: 'USDC_USDT',
    241: 'USDC_XMR',
    240: 'USDC_XRP',
    245: 'USDC_ZEC',
    256: 'USDC_DASH',
    257: 'USDC_EOS',
    258: 'USDC_ETC',
    255: 'USDT_ATOM',
    212: 'USDT_BAT',
    191: 'USDT_BCH',
    121: 'USDT_BTC',
    122: 'USDT_DASH',
    216: 'USDT_DOGE',
    203: 'USDT_EOS',
    173: 'USDT_ETC',
    149: 'USDT_ETH',
    217: 'USDT_GNT',
    218: 'USDT_LSK',
    123: 'USDT_LTC',
    231: 'USDT_MANA',
    124: 'USDT_NXT',
    223: 'USDT_QTUM',
    175: 'USDT_REP',
    219: 'USDT_SC',
    125: 'USDT_STR',
    126: 'USDT_XMR',
    127: 'USDT_XRP',
    180: 'USDT_ZEC',
    220: 'USDT_ZRX',
    259: 'USDT_BCHSV',
    260: 'USDT_BCHABC',
    261: 'USDT_GRIN',
    262: 'USDT_DGB',
}

class PoloniexAuth(requests.auth.AuthBase):

    def __init__(self, auth_data):
        self.api_key = auth_data['poloniex']['api_key']
        self.api_secret = auth_data['poloniex']['api_secret']

    def _sign_data(self, data):
        sig = hmac.new(self.api_secret.encode(), data.encode(), 'sha512')
        return sig.hexdigest()

    def __call__(self, request):
        nonce = int(time.time()*1000)
        request.body += f'&nonce={nonce}'
        sig = self._sign_data(request.body)
        request.headers.update({ 'Key': self.api_key, 'Sign': sig })
        return request

class PoloniexAPI():
    ENDPOINT = 'https://poloniex.com/tradingApi'
    EXCHANGE = 'poloniex'

    def __init__(self, auth_data):
        self.auth = PoloniexAuth(auth_data)

    def _auth_request(self, path, data={}):
        data['command'] = path
        resp = requests.post(f'{self.ENDPOINT}', data=data, auth=self.auth)
        resp.raise_for_status()
        return resp.json()

    def _public_request(self, path, params={}):
        params['command'] = path
        resp = requests.get('https://poloniex.com/public', params=params)
        resp.raise_for_status()
        return resp.json()

    def get_fees(self):
        fees = self._auth_request('returnFeeInfo')
        return common.Fees(Decimal(fees['makerFee']), Decimal(fees['takerFee']))

    # It appears that poloniex has a fixed precision for all currencies of 8 decimal places; likewise,
    # a min base amount of .0001?
    def get_products(self):
        currencies = self._public_request('returnTicker')
        products = []
        for currency in currencies:
            quote, base = currency.split('_')
            products.append(common.ProductInfo(POLONIEX_PAIRS[currencies[currency]['id']], base, quote, Decimal(10)**-8, Decimal(10)**-8, Decimal(1)))
        return products

    def get_wallet_balance(self, asset):
        balance = [ Decimal(balance) for _asset, balance in self._auth_request('returnBalances').items() if _asset == asset ][0]
        return common.Wallet(asset, balance)

    def _order(self, pair, side, price, quantity, **kwargs):
        data = {
            'currencyPair': self.convert_pair(pair),
            'rate': price,
            'amount': quantity
        }

        if 'post_only' in kwargs:
            data['postOnly'] = '1'

        if 'IOC' in kwargs:
            data['immediateOrCancel'] = '1'

        if 'FOK' in kwargs:
            data['fillOrKill'] = '1'

        resp = self._auth_request(side, data=data)
        if 'error' in resp:
            raise common.ExchangeException('poloniex', resp['error'])
        return common.Order(resp['orderNumber'], 'limit', side, Decimal(quantity), price=Decimal(price))

    def limit_buy_order(self, pair, price, quantity, **kwargs):
        return self._order(pair, 'buy', price, quantity, **kwargs)

    #def market_buy_order(self, pair, quantity, **kwargs):
    #    return self._order(pair, 'buy', self.MARKET_BUY_PRICE, quantity, **kwargs)

    def limit_sell_order(self, pair, price, quantity, **kwargs):
        return self._order(pair, 'sell', price, quantity, **kwargs)

    #def market_sell_order(self, pair, quantity, **kwargs):
    #    return self._order(pair, 'sell', self.MARKET_SELL_PRICE, quantity, **kwargs)

    def cancel_order(self, order_id, **kwargs):
        data = {
            'orderNumber': order_id
        }

        try:
            return self._auth_request('cancelOrder', data=data)
        except requests.HTTPError as e:
            raise common.ExchangeException('poloniex', e.response.text)

    @staticmethod
    def convert_pair(pair):
        # Poloniex lists pairs as quote/base, whereas most other exchanges use base/quote, so flip them
        quote, base = pair.split('/')
        return base + '_' + quote

class PoloniexWebsocket():
    WEBSOCKET = 'wss://api2.poloniex.com'
    EXCHANGE = 'poloniex'
    
    ACCOUNT_CHANNEL = 1000
    TICKER_CHANNEL = 1002
    DAILY_VOL_CHANNEL = 1003
    HEARTBEAT_CHANNEL = 1010

    def __init__(self, ws):
        self.ws = ws
        self.queue = asyncio.Queue()
        self.queue_event_task = asyncio.create_task(self._queue_events())
        # order_id: (price,remaining_quantity)
        self.orders = {}

    async def _queue_events(self):
        try:
            async for event in self.ws:
                # May return a list, so add each event separately to the queue
                #for parsed_event in wrappers.PoloniexWebsocketWrapper.parse(json.loads(event)):
                for parsed_event in self.parse(json.loads(event)):
                    if type(parsed_event) == list:
                        for subevent in parsed_event:
                            self.queue.put_nowait(subevent)
                    else:
                        self.queue.put_nowait(parsed_event)
        except asyncio.CancelledError:
            raise

    @staticmethod
    async def connect():
        ws = await websockets.connect(PoloniexWebsocket.WEBSOCKET)
        return PoloniexWebsocket(ws)

    async def subscribe(self, channel):
        await self.ws.send(json.dumps({
            'command': 'subscribe',
            'channel': channel
        }))

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()

    async def subscribe_user_feed(self, auth_data, **kwargs):
        auth = PoloniexAuth(auth_data)
        data = f'nonce={int(time.time()*1000)}'

        await self.ws.send(json.dumps({
            'command': 'subscribe',
            'channel': self.ACCOUNT_CHANNEL,
            'key': auth.api_key,
            'payload': data,
            'sign': auth._sign_data(data)
        }))

    async def subscribe_orderbook_feed(self, pair):
        pair = PoloniexAPI.convert_pair(pair)
        await self.subscribe(pair)

    def parse(self, data):
        #print(data)
        if data[0] == 1000 and len(data) > 2:
            return self.parse_order(data)
        #elif re.fullmatch('[A-Z]+_[A-Z]+', str(data[0])):
        elif data[0] in POLONIEX_PAIRS:
            return self.parse_orderbook(data)
        else:
            return []

    def parse_orderbook(self, data):
        # poloniex is a little bit tricky, because it can sometimes reset the orderbook.
        # In a case like that, you could just return an OrderbookSnapshot event, and use it to reset
        # the book. Then, append any other updates after, which can be sent individually
        BIDS = 1
        ASKS = 0
        updates = []
        ob_updates = []
        for update in data[2]:
            update_type = update[0]

            if update_type == 'i':
                ob = pd.Series({
                    'bids': pd.DataFrame(update[1]['orderBook'][BIDS].items(), columns=['price', 'quantity']),
                    'asks': pd.DataFrame(update[1]['orderBook'][ASKS].items(), columns=['price', 'quantity'])
                })
                updates.append(common.OrderbookSnapshotEvent('poloniex', ob))
            elif update_type == 'o':
                _, side, price, quantity = update
                ob_updates.append([ 'bids' if side == BIDS else 'asks', price, quantity ])

        updates.append(common.OrderbookEvent('poloniex', ob_updates))
        return updates

    def parse_order(self, data):
        updates = []
        message_order = [ 'b', 'p', 'n', 't', 'k', 'o' ]

        data[2] = sorted(data[2], key=lambda key: message_order.index(key[0]))
        print(data[2])

        for message in data[2]:
            #print(f'poloniex message: {message[0]}')
            if message[0] == 'p':
                _, order_id, _, price, quantity, _, _ = message
                self.orders[order_id] = (Decimal(price), Decimal(quantity))
            elif message[0] == 'b':
                pass
            elif message[0] == 'n': # This event only occurs if the order sits on the book
                _, _, order_id, _, price, quantity, _, _, _ = message
                price, quantity = Decimal(price), Decimal(quantity)
                # Not sure if a trade event will be generated
                #self.orders[order_id] = (price, quantity)
                updates.append(common.OrderOpenEvent('poloniex', order_id, price, quantity))
            elif message[0] == 'o':
                _, order_id, new_quantity, update_type, _ = message
                new_quantity = Decimal(new_quantity)
                #self.orders[order_id] = (self.orders[order_id][0], new_quantity)

                #print(f'[o] id: {message[0][1]} new_amount: {message[0][2]} type: {message[0][3]} client_id: {message[0][4]}')

                if update_type == 'f':
                    if new_quantity == 0:
                        updates.append(common.OrderDoneEvent('poloniex', order_id, 'filled'))
                        del self.orders[order_id]
                    #else:
                    #    updates.append(common.OrderMatchEvent('poloniex', order_id, self.orders[order_id][0], new_quantity))
                elif update_type == 'c':
                    if new_quantity == 0:
                        updates.append(common.OrderDoneEvent('poloniex', order_id, 'cancelled'))
                    else:
                        # This is tricky -- think about how to handle
                        # Not even sure I want to produce a fake event, since it should already appear in the pipeline 
                        # if a trade happens
                        #updates.append(common.OrderMatchEvent('poloniex', order_id, 0, new_quantity))
                        updates.append(common.OrderDoneEvent('poloniex', order_id, 'cancelled'))
                    del self.orders[order_id]
            elif message[0] == 't':
                _, _, price, quantity, _, _, order_id, _, _, _ = message
                price, quantity = Decimal(price), Decimal(quantity)
                updates.append(common.OrderMatchEvent('poloniex', order_id, price, quantity))
                self.orders[order_id] = (price, self.orders[order_id][1] - quantity)

                # Poloniex doesn't seem to send a fill order update message if a transaction occurs in a single trade
                if self.orders[order_id][1] == 0 and [ update for update in data[2] if update[0] == 'o' ] == []:
                    updates.append(common.OrderDoneEvent('poloniex', order_id, 'filled'))
                    del self.orders[order_id]
            elif message[0] == 'k':
                _, order_id, _ = message
                updates.append(common.OrderDoneEvent('poloniex', order_id, 'killed'))
                del self.orders[order_id]
            else:
                raise ValueError(f'Unrecognized order event: {message[0]}')

        return updates
