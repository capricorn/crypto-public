'''
Forward arbitrage using option contracts and positions on ledgerX and kraken
Possibly utilizing one-sided mm on ledgerx

Information we need:
    - Realtime option book information
    - OB feed from kraken 
    - Ability to place/cancel orders on ledgerx 

In the simplest case, could probably get away with simply watching the bbo of each contract,
and using it to place limit orders, etc.

Obtain active contracts via the book
'''

import asyncio
import datetime
import json
import base64
import time
from math import ceil
from collections import namedtuple
from itertools import product

import websockets
import requests

from api import ledgerx
from util import util

# The given strategy for MM'ing a forward 
MM_SWAP = 0
MM_CALL = 1
MM_PUT = 2
MIN_PROFIT = 1 # cents

STATE_WAIT_FOR_PROFIT = 0
STATE_WAIT_FOR_MATCH = 1
STATE_BUY_FORWARD = 2
STATE_DONE = 3

ContractStats = namedtuple('ContractStats', 'id label open_interest type ask bid vwap volume')

def get_24hr_contract_stats():
    date = datetime.date.today() - datetime.timedelta(days=1)
    data = requests.get(f'https://data.ledgerx.com/json/{date}.json').json()
    return [ (ContractStats(*contract.values()) if contract['contract_type'] == ledgerx.SWAP else 
        ContractStats(*list(contract.values())[:-1])) for contract in data['report_data'] ]

def optimal_orders_rec(orders, volume, max_cost, max_orders, cur_cost, cur_subset):
    if orders == [] or len(orders) == max_orders:
        return cur_subset

    while orders != []:
        best = max(orders, key=lambda strat: (strat.profit / strat.cost) * \
                ((volume[strat.swap.id] + volume[strat.forward.call.id] + volume[strat.forward.put.id]) / sum(list(volume.values()))))

        if best.cost + cur_cost > max_cost:
            orders.remove(best)
        else:
            cur_subset.append(best)
            cur_cost += best.cost
            break

    return optimal_orders_rec(orders, volume, max_cost, max_orders, cur_cost, cur_subset)

def optimal_orders(orders, volume, max_cost, max_orders):
    #ppc = list(map(lambda strat: strat.profit / strat.cost, strats))
    #best = max(orders, key=lambda strat: strat.profit / strat.cost)
    return optimal_orders_rec(orders, volume, max_cost, max_orders, 0, [])

def optimal_ordersv2(orders, balance, volumes):
    total_vol = sum(list(volumes.values()))

    # Each contract have access to its 24 hr volume?
    margin_req = max([order.cost - order.initial_cost for order in orders]) * 1.05
    costs = [ order.initial_cost for order in orders ]
    # Problem: Introduces decimal numbers (for now, leave out denominator) 
    values = [ order.profit * (cum_vol(order.contracts, volumes)) for order in orders ]

    _, order_idxs = util.knapsack(len(orders)-1, costs, values, balance - margin_req, [], {})
    print(f'order idxs: {order_idxs}')

    return [ orders[i] for i in order_idxs ]

def mfa_auth():
    with open('mfa.txt', 'r') as f:
        token = f.read()
        if token != '':
            token_data = json.loads(base64.b64decode(token.split('.')[1] + '==').decode())
            if token_data['exp'] > time.time():
                return token

    print('JWS token expired. Requesting new one.')
    mfa_token = ledgerx.LedgerXAPI.get_mfa_token(*ledgerx.read_auth_file('auth.json'))
    token = ledgerx.LedgerXAPI.get_jws_token(mfa_token, input('Enter mfa code: '))

    with open('mfa.txt', 'w') as f:
        f.write(token)

    return token

def cum_vol(contract_ids, volumes):
    return sum([volumes[contract_id] for contract_id in contract_ids])

async def main():
    api = ledgerx.LedgerXAPI(mfa_auth())
    book = ledgerx.LedgerXBook(api.get_contracts(), api.get_contracts_bbo())
    balance = None

    #volume = { contract.id: contract.volume + 1 for contract in get_24hr_contract_stats() }
    volume = { contract_id: 1 for contract_id in book.contracts }
    for contract in get_24hr_contract_stats():
        volume[contract.id] = contract.volume + 1

    forwards = []
    for _, pairs in book.strikes.items():
        for pair in pairs:
            forwards.append(ForwardContract(*pair))

    # From the book, locate today's swap (use live date)
    today = datetime.datetime.today()
    current_swap = list(filter(lambda swap: today.date() >= swap.live.date() and swap.underlying == 'CBTC' and swap.active, 
        book.swaps.values()))[0]

    for contract in book.contracts.values():
        print(contract.id, contract.label)
    print(f'Current swap: {current_swap.label}')
    # First item is the previous swap -- estimate today's swap volume from this
    volume[current_swap.id] = list(volume.values())[0]
    #del volume[list(volume.keys())[0]]

    mm_method_name = {
        MM_SWAP: 'MM Swap',
        MM_CALL: 'MM Call',
        MM_PUT: 'MM Put'
    }

    strats = [ ArbSellMMSwap, ArbSellMMPut ]

    forwards = list(filter(lambda f: (f.expires.date() - current_swap.expires.date()).days <= 30, forwards))
    arbs = []

    for Strat in strats:
        arbs.extend([ Strat(api, current_swap, forward) for forward in forwards ])

    # This is now a strat object
    current_forward = None
    active_order = None

    active_orders = []
    basket_cancel = set()

    STATE_CREATE_ORDERS = 0
    STATE_OPTIMIZE_ORDERS = 1
    STATE_UPDATE_ORDERS = 2
    STATE_BASKET_ADJUSTMENT = 3

    state = STATE_CREATE_ORDERS

    # SM to manage basket of arbitrage strategies 
    async with websockets.connect(f'wss://trade.ledgerx.com/api/ws?token={api.token}') as ws:
        while True:
            msg = json.loads(await ws.recv())

            if 'collateral' in msg:
                available = msg['collateral']['available_balances']
                balance = [ available['USD'], available['CBTC'] ]
                # Pretend we have $200
                #balance = [ 20000, available['CBTC'] ]
                print(f'USD balance {balance[0]} BTC balance {balance[1]}')

            if 'type' in msg and msg['type'] == 'book_top':
                book.update(msg['contract_id'], ledgerx.BBO(msg['bid'], msg['ask']))
                if not balance: continue

            all_orders = len(active_orders)
            active_orders = [ order for order in active_orders if order.state not in [StratOrder.STATE_CANCELLED, StratOrder.STATE_DONE] ]

            if state == STATE_BASKET_ADJUSTMENT:
                if set(active_orders) & basket_cancel == set():
                    state = STATE_CREATE_ORDERS
            elif len(active_orders) < all_orders:
                state = STATE_CREATE_ORDERS

            # Setup our basket of orders
            if state == STATE_CREATE_ORDERS and 'type' in msg and msg['type'] == 'book_top':
                forwards = [ arb for arb in arbs if arb.cost <= balance[0] and arb.valid ]
                print(f'Strats to choose from: {len(forwards)}')

                if forwards == []:
                    print('No contract in our budget.')
                    print(f'Strat profits: {[arb.profit for arb in forwards]}')
                    print(f'Strat costs: {[arb.cost for arb in forwards]}')
                else:
                    print(f'Strat profits: {[arb.profit for arb in forwards]}')
                    print(f'Strat costs: {[arb.cost for arb in forwards]}')
                    print(f'Profitable: {[arb for arb in forwards if arb.profit > 0]}')
                    #optimal = optimal_orders(forwards, volume, balance[0], 3)
                    optimal = optimal_ordersv2(forwards, balance[0], volume)
                    print(f'Optimal orders: {[order.profit for order in optimal]}')
                    print(f'Optimal order cost: {sum([order.initial_cost for order in optimal])}')

                    # Obtain the most profitable forward
                    #best_forward = max(forwards, key=lambda arb: arb.profit)
                    best_forwards = optimal[0:min(len(optimal), 2)]
                    # Find the orders no longer considered optimal (in order to do so, must support comparison)
                    basket_cancel = set(active_orders) - set(best_forwards)
                    if basket_cancel != set():
                        # Here, we will need to dispatch cancel() code on orders.
                        # This is where internal event queue for orders will be necessary 
                        # Somewhat tricky though, since we would like to know when this group of orders has successfully cancelled
                        # active_orders & cancel == {} is when we can continue (extra state?)
                        print(f'Removing item(s) from basket: {basket_cancel}')
                        state = STATE_BASKET_ADJUSTMENT
                        continue

                    print(f'best: {best_forwards}')
                    for best_forward in best_forwards:
                        print(f'Most profitable arb: {best_forward.profit} {best_forward}')

                        if best_forward.profit > MIN_PROFIT:
                            active_order = best_forward.create_order()
                            asyncio.create_task(active_order.run())
                            active_orders.append(active_order)
                            state = STATE_UPDATE_ORDERS

            elif state == STATE_UPDATE_ORDERS:
                for active_order in active_orders:
                    asyncio.create_task(active_order.update(msg, msg['type']))

            '''
            elif state == STATE_OPTIMIZE_ORDERS:
                strats = [ arb for arb in arbs if arb.cost <= balance[0] and arb.valid ]
                if strats == []:
                    print(f'Current arb basket cannot be optimized any more.')
                    state = STATE_UPDATE_ORDERS
            '''

    await api.close()

if __name__ == '__main__':
    asyncio.run(main())
