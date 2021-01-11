import asyncio
import logging

from decimal import Decimal as D

from api import CoinbaseAPI
from api import ob as simpleob
from api import trade
from util import util

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

async def main():
    feed = await trade.create_feed('coinbase', 'REP/USD')
    logger.debug('Established trade feed')
    ob = feed['coinbase']['ob']
    ws = feed['coinbase']['ws']

    await ws.subscribe('REP/USD', 'full')
    logger.debug('Subscribed to full feed')
    
    buy_orders = []
    open_orders = {}
    # Track time on book until a match occurs?
    async for event in ws:
        if event.event_type == 'order_open' and event.side == 'buy':
            open_orders[event.id] = event.timestamp
            logger.debug(f'Opened order {event.id} at {event.timestamp}')

        elif event.event_type == 'order_match' and event.side == 'buy' and event.id in open_orders:
            logger.debug(f'Time till match: {event.timestamp - open_orders[event.id]}')
            del open_orders[event.id]

        elif event.event_type == 'order_done' and event.side == 'buy' and event.id in open_orders:
            logger.debug(f'Removing order {event.id}: {event.timestamp - open_orders[event.id]}')
            del open_orders[event.id]

        #if event.event_type == 'orderbook_update':
        #    logger.debug(f'BBO: {ob.best_bid()}/{ob.best_ask()} ({event.timestamp})')

        # Next, log cancelled orders (& its spread at the time)
        # Maybe also consider length of time an order was on the book as data? (Perhaps as a function of spread)
        '''
        if event.event_type == 'order_received' and event.side == 'buy' and event.order_type == 'limit':
            # Actually, save any bids where price < ob.best_ask(), and calculate the spread ob.best_ask() - price
            spread = ob.best_ask()[0]-event.price
            logger.debug(f'Order spread: {spread}')
            if spread > 0:
                logger.debug(f'{event.price},{event.quantity},{event.timestamp},{spread} -- {len(buy_orders)}')
                buy_orders.append(f'{event.price},{event.quantity},{event.timestamp},{spread}\n')
        '''

        '''
        if event.event_type == 'order_done' and event.side == 'buy':
            logger.debug(f'{event.reason}')
            if event.reason == 'canceled':
                logger.debug(f'{event.timestamp}')
                buy_orders.append(f'{event.timestamp}\n')

        if len(buy_orders) == 100:
            break
        '''

    logger.debug('Recording buy orders')
    with open('cancel_orders.csv', 'w') as f:
        for order in buy_orders:
            f.write(order)
    logger.debug('Finished recording orders.')

asyncio.run(main())
