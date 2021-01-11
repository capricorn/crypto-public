import json
import datetime
from itertools import groupby
from collections import namedtuple

import requests
import aiohttp

API = 'https://trade.ledgerx.com/api'
SWAP = 'day_ahead_swap'
OPTION = 'options_contract'

STATUS_TRADE = 201
STATUS_CANCELLED = 203

Contract = namedtuple('Contract', 'id label underlying collateral active type strike live expires exercise derivative increment')
BBO = namedtuple('BBO', 'bid ask')

def read_auth_file(credential_file):
    with open(credential_file, 'r') as f:
        auth_data = json.loads(f.read())
        return (auth_data['ledgerx']['username'], auth_data['ledgerx']['password'])

# Eventually just replace contract with this
class LedgerXContract():
    def __init__(self, contract, bbo):
        self.label = contract.label
        self.bbo = bbo
        self.strike = contract.strike
        self.id = contract.id
        self.exercise = contract.exercise
        self.expires = contract.expires
        self.live = contract.live
        self.underlying = contract.underlying
        self.active= contract.active
        self.type = contract.type

    @property
    def bid(self):
        return self.bbo[self.id].bid

    @property
    def ask(self):
        return self.bbo[self.id].ask

class Option(LedgerXContract):
    pass

class Swap(LedgerXContract):
    pass

class LedgerXBook():
    def __init__(self, contracts, initial_bbo):
        # Do we want to filter by contracts that are active? (And also have the correct underlying)
        self.contracts = contracts
        # Strikes should contain the pair of put/call contracts for that strike
        #self.strikes = { contract.strike: contract for _, contract in contracts.items() if contract.strike }

        self.strikes = {}
        self.bbo = initial_bbo
        self.options = { contract_id: Option(contract, self.bbo) for contract_id, contract in contracts.items() if contract.derivative == OPTION }
        self.swaps = { contract_id: Swap(contract, self.bbo) for contract_id, contract in contracts.items() if contract.derivative == SWAP }


        for strike, strike_options in groupby(sorted(self.options.values(), key=lambda option: option.strike), key=lambda option: option.strike):
            sorted_strikes = sorted(list(strike_options), key=lambda option: option.exercise)
            strike_pairs = []
            for _, option_pair in groupby(sorted_strikes, key=lambda option: option.exercise.date()):
                strike_pairs.append(sorted(tuple(option_pair), key=lambda option: 0 if option.type == 'call' else 1))

            self.strikes[strike] = strike_pairs

    def update(self, contract_id, bbo):
        self.bbo[contract_id] = BBO(bbo.bid // 100, bbo.ask // 100)

# Perhaps make this class usable with a context manager, to clean things up after?
class LedgerXAPI():
    def __init__(self, username, password, mfa_code):
        self.session = requests.Session()

        mfa_token = self.get_mfa_token(*read_auth_file('auth.json'))
        self.token = self.get_jws_token(mfa_token, mfa_code)

        self.session.headers.update({
            'authorization': f'JWT {self.token}',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
        })

    def __init__(self, mfa_token):
        self.token = mfa_token
        self.session = aiohttp.ClientSession(headers={'authorization': f'JWT {self.token}'})

    async def close(self):
        await self.session.close()

    @staticmethod
    def get_mfa_token(username, password):
        data = {
            'username': username,
            'password': password
        }

        resp = requests.post(f'{API}/auth', data=data)
        resp.raise_for_status()
        return resp.json()['data']['mfaToken']

    @staticmethod
    def get_jws_token(mfa_token, mfa_code):
        data = {
            'requestId': mfa_token,
            'totpToken': mfa_code
        }
        resp = requests.post(f'{API}/auth/mfa', data=data)
        resp.raise_for_status()
        return resp.json()['data']['token']

    # update for asyncio
    def get_contract_book(self, contract_id):
        # Maybe place this inside an auth_request method, calls raise_for_status, etc
        resp = self.session.get(f'{API}/book-states/{contract_id}')
        resp.raise_for_status()
        return resp.json()

    # What if this was a classmethod..?
    @staticmethod
    def get_contracts():
        # 06:00 UTC is 1am EST, which (apparently) is what ledgerX uses to decide whether a contract is valid for that day
        # Presumably it obtains contracts by comparing their expiration date to the provided timestamp
        resp = requests.get(f'{API}/contracts?after_ts={datetime.date.today().isoformat()}T06:00:00.000Z&limit=0')
        resp.raise_for_status()
        return { contract['id']: LedgerXAPI._init_contract(contract) for contract in resp.json()['data'] }

    @staticmethod
    def get_contracts_bbo():
        resp = requests.get(f'{API}/book-tops')
        resp.raise_for_status()
        return { contract['contract_id']: BBO(contract['bid'] // 100, contract['ask'] // 100) for contract in resp.json()['data'] }

    @staticmethod
    def _init_contract(contract):
        keys = [
            'id', 'label', 'underlying_asset', 'collateral_asset', 'active', 'type', 'strike_price', 'date_live',
            'date_expires', 'date_exercise', 'derivative_type', 'min_increment'
        ]

        if contract['derivative_type'] == SWAP: 
            contract['type'] = None
            contract['strike_price'] = None
        elif contract['derivative_type'] == OPTION:
            contract['date_exercise'] = datetime.datetime.strptime(contract['date_exercise'], '%Y-%m-%d %H:%M:%S+0000')
            contract['strike_price'] //= 100

        contract['date_expires'] = datetime.datetime.strptime(contract['date_expires'], '%Y-%m-%d %H:%M:%S+0000')
        contract['date_live'] = datetime.datetime.strptime(contract['date_live'], '%Y-%m-%d %H:%M:%S+0000')
        contract['min_increment'] *= 100

        return Contract(*[ contract[key] for key in keys ])
    
    async def _order(self, order_type, order_side, contract_id, quantity, price=None):
        data = {
            'order_type': order_type,
            'contract_id': contract_id,
            'is_ask': order_side == 'ask',
            'swap_purpose': 'undisclosed',
            'size': quantity
        }

        if order_type == 'limit':
            data['price'] = price

        #resp = self.session.post(f'{API}/orders', data=data)
        async with self.session.post(f'{API}/orders', data=data) as resp:
            body = await resp.json()
            return body['data']['mid']
        #resp.raise_for_status()
        #return resp.json()['data']['mid']

    async def limit_buy(self, contract_id, price, quantity):
        return await self._order('limit', 'bid', contract_id, quantity, price=price)

    async def limit_sell(self, contract_id, price, quantity):
        return await self._order('limit', 'ask', contract_id, quantity, price=price)

    async def market_buy(self, contract_id, quantity):
        return await self._order('market', 'bid', contract_id, quantity)

    async def market_sell(self, contract_id, quantity):
        return await self._order('market', 'ask', contract_id, quantity)
    
    async def cancel_order(self, order_id, contract_id):
        data = {
            'contract_id': contract_id
        }

        async with self.session.delete(f'{API}/orders/{order_id}', data=data) as resp:
            body = await resp.json()
            return body['data']['success']
        '''
        resp = self.session.delete(f'{API}/orders/{order_id}', data=data)
        resp.raise_for_status()
        return resp.json()
        '''
