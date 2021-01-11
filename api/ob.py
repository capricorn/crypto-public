import sys
import heapq
import time
from decimal import Decimal
from collections import namedtuple, Counter

import numpy as np
import pandas as pd

# Have to override all comparison operators, etc..
class NormalizedQuantity():
    pass

class Orderbook():
    OrderbookEntry = namedtuple('OrderbookEntry', 'price time quantity')

    def __init__(self, snapshot, quote_prec, base_prec):
        # Set datatype to ticks class
        pass

    def __init__(self, snapshot):
        self.quotetype, self.basetype = Decimal, Decimal
        # Utilize named tuples to make things a little more readable
        self.orderbook = {
            'asks': [ (Decimal(ask.price), time.time_ns(), Decimal(ask.quantity)) for ask in snapshot['asks'].itertuples() ],
            'bids': [ (-Decimal(bid.price), time.time_ns(), Decimal(bid.quantity)) for bid in snapshot['bids'].itertuples() ]
        }

        heapq.heapify(self.orderbook['asks'])
        heapq.heapify(self.orderbook['bids'])

        self.entry_count = {
            'asks': Counter([ ask[0] for ask in self.orderbook['asks'] ]),
            'bids': Counter([ bid[0] for bid in self.orderbook['bids'] ])
        }

    # How is it possible for this to endlessly recurse?
    def _heap_peek(self, side):
        heap = self.orderbook[side]
        while True:
            level = heap[0]   # Get the oldest entry at this price level

            # Lazy delete all old entries at this price level
            while self.entry_count[side][level[0]] > 1:
                heapq.heappop(heap)
                #print(f'Removing old entry: {heapq.heappop(heap)}')
                self.entry_count[side].subtract([level[0]])

            level = heap[0] # Newest entry in heap
            if level[2] == 0:
                del self.entry_count[side][level[0]]
                #print(f'Deleting dead entry: {heapq.heappop(heap)}')
                # Possible a deletion may be sent for an item no longer in the book?
                dead = heapq.heappop(heap)
                #print(f'Deleting dead entry: {dead}')
                continue
                #return self._heap_peek(side)

            return level

    # Any fast way to check for entries..? May need to back via a dictionary
    def update(self, update, max_depth=None):
        for side, price, quantity in update:
            price = -Decimal(price) if side == 'bids' else Decimal(price)
            quantity = Decimal(quantity)

            self.entry_count[side].update([price])
            heapq.heappush(self.orderbook[side], (price, time.time_ns(), quantity))

    def best_bid(self):
        level = self._heap_peek('bids')
        return (-level[0], level[2])

    def best_ask(self):
        level = self._heap_peek('asks')
        return (level[0], level[2])

# I don't think sorting is necessary with min()
def drop_price_level(ob, side, level):
    drop_row = ob[side][ob[side]['price'] == level]
    if drop_row.empty: 
        return
    ob[side] = ob[side].drop(drop_row.index)

def add_price_level(ob, side, level, quantity):
    ob[side] = ob[side].append(pd.Series([level, quantity],
        index=['price', 'quantity']),
            ignore_index=True)

def change_price_level(ob, side, level, new_quantity):
    ob[side]['quantity'][ob[side]['price'] == level] = new_quantity

def get_best_bid(ob):
    max_price = max(ob['bids']['price'].array, key=lambda k: round(float(k), precision(k)))
    bid = ob['bids'][ob['bids']['price'] == max_price]
    return (bid['price'].array[0], bid['quantity'].array[0])

def get_best_ask(ob):
    min_price = min(ob['asks']['price'].array, key=lambda k: round(float(k), precision(k)))
    ask = ob['asks'][ob['asks']['price'] == min_price]
    return (ask['price'].array[0], ask['quantity'].array[0])

def convert_coinbase_ob(snapshot):
    ob = pd.Series({
        'bids': pd.DataFrame(snapshot['bids'], columns=['price','quantity']),
        'asks': pd.DataFrame(snapshot['asks'], columns=['price','quantity'])})
    return ob

def convert_kraken_ob(snapshot):
    ob = pd.Series({
        'bids': pd.DataFrame(snapshot[1]['bs'], columns=['price','quantity', 'time']),
        'asks': pd.DataFrame(snapshot[1]['as'], columns=['price','quantity', 'time'])})
    ob['bids'] = ob['bids'].drop(columns='time')
    ob['asks'] = ob['asks'].drop(columns='time')
    return ob

def convert_kraken_update(update):
    updates = []
    update = list(filter(lambda k: type(k) == dict, update))
    for change in update:
        if 'b' in change:
            updates.extend(list(map(lambda k: ['bids', k[0], k[1]], change['b'])))
        if 'a' in change:
            updates.extend(list(map(lambda k: ['asks', k[0], k[1]], change['a'])))
    return updates

def convert_coinbase_update(update):
    update = update['changes']
    return list(map(lambda k: ['bids' if k[0] == 'buy' else 'asks', k[1], k[2]], update))

# Updates are in the form [ [side, price, quantity]+ ]
def apply_ob_update(ob, update, max_depth=None):
    ob = ob.copy()
    for side, price, quantity in update:
        if float(quantity) == 0:
            drop_price_level(ob, side, price)
        elif price in ob[side]['price'].array:
            change_price_level(ob, side, price, quantity)
        else:
            add_price_level(ob, side, price, quantity)

    # If the length of the book is too large after application of our updates,
    # then just sort the book, and purge the appropriate items (min or max) until
    # a proper depth is reached again.
    # This is a naive approach, but should work for now.
    if max_depth:
        while True:
            if len(ob['bids']) > max_depth:
                drop_price_level(ob, 'bids', ob['bids']['price'].min())
            elif len(ob['asks']) > max_depth:
                drop_price_level(ob, 'asks', ob['asks']['price'].max())
            else: 
                break
    return ob

def precision(num):
    try:
        return len(num[num.index('.'):])
    except ValueError:
        return 0
