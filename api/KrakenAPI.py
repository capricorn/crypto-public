import time
import hmac
import base64
import hashlib
import urllib

import requests
from api import common
from api import ob as simpleob

class KrakenAPI():
    API = 'https://api.kraken.com'
    WEBSOCKET = 'wss://ws.kraken.com'

    def __init__(self, auth_data):
        self.session = requests.Session()
        self.session.headers.update({
            'API-Key': auth_data['kraken']['api_key']
        })
        self.api_key = auth_data['kraken']['api_key']
        self.api_secret = auth_data['kraken']['api_secret']

    def _api_signature(self, endpoint, req_data, nonce):
        postdata = urllib.parse.urlencode(req_data)

        encoded = (str(nonce) + postdata).encode()
        message = endpoint.encode() + hashlib.sha256(encoded).digest()

        signature = hmac.new(base64.b64decode(self.api_secret),
                             message, hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest())
        return sigdigest

    def _post_request(self, endpoint, data={}):
        data['nonce'] = int(1000*time.time())
        headers = {
            'API-Sign': self._api_signature(endpoint, data, data['nonce'])
        }
        resp = self.session.post(KrakenAPI.API + endpoint, headers=headers, data=data)
        resp.raise_for_status()
        return resp.json()

    def get_account_balance(self):
        return self._post_request('/0/private/Balance')

    def get_trade_balance(self):
        pass

    def settle_sell_position(self, pair, volume, validate=True):
        order = {
            'pair': pair,
            'type': 'sell',
            'ordertype': 'settle-position',
            'leverage': 2,  # Any leverage is accepted
            'volume': volume,
            'validate': validate
        }

        return self._post_request('/0/private/AddOrder', data=order)

    def _order(self, side, ordertype, pair, volume, price=None, leverage=None, validate=True, oflags=[]):
        order = {
            'pair': pair,
            'type': side,
            'ordertype': ordertype,
            'volume': volume,
        }

        if validate:
            order['validate'] = True

        if leverage:
            order['leverage'] = leverage

        if price:
            order['price'] = price

        if oflags != []:
            order['oflags'] = ','.join(oflags)


        resp = self._post_request('/0/private/AddOrder', data=order)
        if resp['error'] != []:
            raise common.OrderException(resp['error'])

        return common.Order(resp['result']['txid'][0], ordertype, side, volume, price)

    def market_buy_order(self, pair, volume, leverage=None, validate=True):
        return self._order('buy', 'market', pair, volume, leverage=leverage, validate=validate)

    def market_sell_order(self, pair, volume, leverage=None, validate=True):
        return self._order('sell', 'market', pair, volume, leverage=leverage, validate=validate)

    def _limit_order(self, side, pair, price, volume, leverage=None, validate=True, oflags=[]):
        return self._order(side, 'limit', pair, volume, leverage=leverage, validate=validate, price=price, oflags=oflags)

    def limit_buy_order(self, pair, price, volume, leverage=None, validate=True, oflags=[]):
        return self._limit_order('buy', pair, price, volume, leverage=leverage, validate=validate, oflags=oflags)

    def limit_sell_order(self, pair, price, volume, leverage=None, validate=True, oflags=[]):
        return self._limit_order('sell', pair, price, volume, leverage=leverage, validate=validate, oflags=oflags)
    
    def cancel_order(self, tx, validate=True):
        order = {
            'txid': tx,
        }
        if validate:
            order['validate'] = True

        return self._post_request('/0/private/CancelOrder', data=order)

    def get_open_orders(self):
        resp = self._post_request('/0/private/OpenOrders')
        return resp

    def get_closed_orders(self):
        pass

    # Note: expires after 60 minutes
    def get_websocket_token(self):
        params = {
            'validity': 60,
            'permissions': 'all'
        }
        return self._post_request('/0/private/GetWebSocketsToken', data=params)['result']['token']
