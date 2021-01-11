from decimal import Decimal
import datetime

from api import common
from api import ob as simpleob
from api import exchange
import pandas as pd

class BinanceWebsocketWrapper():
    @staticmethod
    def parse(data):
        parsers = {
            'depthUpdate': BinanceWebsocketWrapper.parse_orderbook_update,
            'executionReport': BinanceWebsocketWrapper.parse_order,
            'outboundAccountInfo': lambda e: common.Event('account_info', 'binance'),
            'outboundAccountPosition': lambda e: common.Event('account_position', 'binance')
        }

        return parsers[data['e']](data)

    @staticmethod
    def parse_orderbook_update(data):
        updates = []

        updates.extend(list(map(lambda bid: ['bids', *bid], data['b'])))
        updates.extend(list(map(lambda ask: ['asks', *ask], data['a'])))

        return common.OrderbookEvent('binance_us', updates, data['U'])

    @staticmethod
    def parse_order(data):
        # Would be nice to parse data into a named tuple
        #print(f'order: {data}')
        if data['x'] == 'NEW':
            if data['o'] == 'LIMIT':
                return common.OrderOpenEvent('binance', data['i'], data['p'], data['q'])
            elif data['o'] == 'MARKET':
                return common.OrderOpenEvent('binance', data['i'], None)
            elif data['o'] == 'LIMIT_MARKET':
                # TODO 
                pass
        elif data['x'] == 'CANCELED' or data['x'] == 'REJECTED' or data['x'] == 'EXPIRED':
            return common.OrderDoneEvent('binance', data['i'], 'cancelled')
        elif data['x'] == 'TRADE':
            # Need to return a MatchEvent, and then DoneEvent, if the order filled (probably need to integrate into binance websocket to track this)
            return common.OrderMatchEvent('binance', data['i'], data['p'], float(data['z']) - float(data['l']))

class CoinbaseWebsocketWrapper():
    @staticmethod
    def parse(data):
        parsers = {
            'received': CoinbaseWebsocketWrapper.parse_order,   # New order event
            'open': CoinbaseWebsocketWrapper.parse_order,   # Remaining are order update events
            'done': CoinbaseWebsocketWrapper.parse_order,
            'match': CoinbaseWebsocketWrapper.parse_order,
            'heartbeat': CoinbaseWebsocketWrapper.parse_heartbeat,
            'subscriptions': CoinbaseWebsocketWrapper.parse_subscriptions,
            'l2update': CoinbaseWebsocketWrapper.parse_orderbook_update,
            'snapshot': CoinbaseWebsocketWrapper.parse_orderbook_snapshot
        }
        return parsers[data['type']](data)

    @staticmethod
    def parse_subscriptions(data):
        return common.SubscriptionsEvent('coinbase', data)

    @staticmethod
    def parse_heartbeat(data):
        return common.HeartBeatEvent('coinbase')

    @staticmethod
    def parse_order(data):
        if 'time' in data:
            data['time'] = datetime.datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
        if data['type'] == 'received':
            if data['order_type'] == 'limit':
                return common.OrderReceivedEvent('coinbase', data['order_id'], data['price'], quantity=data['size'], order_type='limit', timestamp=data['time'], side=data['side'])
            elif data['order_type'] == 'market':
                # Unsure if I really want to set this as price..
                quantity = data['funds'] if 'funds' in data else 0
                return common.OrderReceivedEvent('coinbase', data['order_id'], 0, quantity=quantity, order_type='market', timestamp=data['time'], side=data['side'])
        elif data['type'] == 'open':
            return common.OrderOpenEvent('coinbase', data['order_id'], data['price'], data['remaining_size'], side=data['side'], sequence=data['sequence'], timestamp=data['time'])
        elif data['type'] == 'done':
            return common.OrderDoneEvent('coinbase', data['order_id'], data['reason'], timestamp=data['time'], side=data['side'], remaining_size=Decimal(data['remaining_size']), price=Decimal(data['price']))
        elif data['type'] == 'match':
            return common.OrderMatchEvent('coinbase', data['trade_id'], data['price'], data['size'], sequence=data['sequence'], timestamp=data['time'])

        raise ValueError(f'Unknown order type: {data["type"]}')

    @staticmethod
    def cb_time_to_timestamp(time):
        return datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()

    @staticmethod
    def parse_orderbook_update(data):
        if data['type'] == 'l2update':
            return common.OrderbookEvent('coinbase', simpleob.convert_coinbase_update(data), timestamp=CoinbaseWebsocketWrapper.cb_time_to_timestamp(data['time']))

        raise ValueError(f'Unknown orderbook update type: {data["type"]}')

    @staticmethod
    def parse_orderbook_snapshot(data):
        if data['type'] == 'snapshot':
            return common.OrderbookSnapshotEvent('coinbase', simpleob.convert_coinbase_ob(data))

        raise ValueError(f'Unknown orderbook snapshot type: {data["type"]}')

class CoinbaseAPIWrapper():
    @staticmethod
    def parse_get_orderbook(data):
        #data['bids'] = [ entry for entry in data['bids'] ]
        #data['asks'] = [ entry for entry in data['asks'] ]

        ob = pd.Series({
            'bids': pd.DataFrame(data['bids'], columns=['price','quantity', 'order_id']),
            'asks': pd.DataFrame(data['asks'], columns=['price','quantity', 'order_id'])})
        return ob

'''
class PoloniexWebsocketWrapper():
    @staticmethod
    def parse(data):
        print(data)
        if data[0] == 1000 and len(data) > 2:
            return PoloniexWebsocketWrapper.parse_order(data)
        else:
            return []

    @staticmethod
    def parse_order(data):
        updates = []
        # Shouldn't return items; instead, append and return a list
        # Also a bit worried about lack of guaranteed order; will this cause problems ..?
        for message in data[2]:
            if message[0] == 'p':
                pass
            elif message[0] == 'b':
                pass
            elif message[0] == 'n': # Newly created order
                _, _, order_id, _, price, quantity, _, _, _ = message
                updates.append(common.OrderOpenEvent('poloniex', order_id, Decimal(price), Decimal(quantity)))
            # Order updates don't seem to work quite right..
            elif message[0] == 'o': # Order update -- sent during cancels, but apparently not during trades? Maybe only for adjustments made by me..? 
                _, order_id, new_quantity, update_type, _ = message
                new_quantity = Decimal(new_quantity)

                if update_type == 'f':  # fill
                    if new_quantity == 0: # Complete fill 
                        updates.append(common.OrderDoneEvent('poloniex', order_id, 'filled'))
                    else:
                        # Since poloniex only support limit orders, it's expected you know the price
                        updates.append(common.OrderMatchEvent('poloniex', order_id, 0, new_quantity))
                elif update_type == 'c': # Order cancel 
                    if new_quantity == 0:   # Complete cancel
                        updates.append(common.OrderDoneEvent('poloniex', order_id, 'cancelled'))
                    else:   # partial cancel
                        updates.append(common.OrderMatchEvent('poloniex', order_id, 0, new_quantity))
                        updates.append(common.OrderDoneEvent('poloniex', order_id, 'cancelled'))

            # Unfortunately. no reasonable way to track order status, unless we manage the order ourselves within the feed (keep a reference to it)
            # Parsers would then need to be implemented within the websocket (or do so in the trade feed..?)
            elif message[0] == 't':
                _, _, price, quantity, _, _, order_id, _, _, _ = message
                updates.append(common.OrderMatchEvent('poloniex', order_id, price, quantity))
            elif message[0] == 'k':
                _, order_id, _ = message
                updates.append(common.OrderDoneEvent('poloniex', order_id, 'killed'))
            else:
                raise ValueError(f'Unrecognized order event: {message[0]}')
        return updates
'''
