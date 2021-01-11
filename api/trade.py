import asyncio
from functools import reduce

import pandas as pd

from api import common
from api import ob as simpleob
from api import CoinbaseAPI as coinbase
from api import BinanceAPI as binance
from api import PoloniexAPI as poloniex
from util import util

'''
Creates a trade feed for a given websocket. Subscribes to user feed and
orderbook update feed. Returns the up-to-date orderbook.
'''
async def create_feed(exchange, pair):
    auth_data = util.read_auth_file('auth.json')
    mapping = {
        'coinbase': {
            'ws': coinbase.CoinbaseWebsocket.connect,
            'api': coinbase.CoinbaseAPI
        },
        'binance_us': {
            'ws': binance.BinanceWebsocket.connect,
            'api': binance.BinanceAPI
        },
        'poloniex': {
            'ws': poloniex.PoloniexWebsocket.connect,
            'api': poloniex.PoloniexAPI
        }
    }
    exws = await mapping[exchange]['ws']()
    api = mapping[exchange]['api'](auth_data)
    #pair = api.convert_pair(pair)

    await exws.subscribe_user_feed(auth_data, pair=pair)
    await exws.subscribe_orderbook_feed(pair)

    # Exchanges that don't send snapshots
    if api.EXCHANGE in ['binance_us']:
        ob = await asyncio.get_event_loop().run_in_executor(None, lambda: api.get_orderbook(pair))

        while not exws.queue.empty():
            event = await exws.queue.get()
            if event.event_type == 'orderbook_update' and event.sequence > ob['sequence']:
                #print('Applying OB update')
                # Apply only if the sequence number is after the orderbook's sequence number
                simpleob.apply_ob_update(ob, event.update)
        return {
            exchange: {
                'ob': simpleob.Orderbook(ob),
                'ws': exws
            }
        }

    else:
        async for event in exws:
            if event.event_type == 'orderbook_snapshot':
                return {
                    exchange: {
                        'ob': simpleob.Orderbook(event.snapshot),
                        'ws': exws
                    }
                }

async def create_feeds(exchanges):
    data = await asyncio.gather(*[ create_feed(exchange, exchanges[exchange]) for exchange in exchanges ])
    return reduce(lambda x,y: { **x, **y }, data)
