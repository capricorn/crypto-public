# More accurately ForwardArbStrat
class ArbStrat():
    def __init__(self, api, swap, forward):
        self.api = api
        self.swap = swap
        self.forward = forward
        self.contracts = { swap.id, forward.call.id, forward.put.id } 

    def profit(self):
        raise NotImplementedError

    def cost(self):
        raise NotImplementedError

    def contracts(self):
        '''
        Return number of available arb contracts that can be purchased
        Need access to the contract orderbook, or somewhere that details contract count
        '''
        raise NotImplementedError

    # True if such a contract can be purchased at this time
    def available(self):
        pass

    def create_order():
        raise NotImplementedError

    def __contains__(self, item):
        return item in self.contracts

class StratOrder():
    STATE_NEW = -1
    STATE_RUNNING = -2
    STATE_CANCELLED = -3
    STATE_DONE = -4

    def __init__(self):
        self.state = self.STATE_NEW
        self.ids = set()

    # Here is where the state machine logic will be implemented
    def update(self):
        raise NotImplementedError

    # Two strats are equal if they share the same type and have the same contract ids
    def __eq__(self, other):
        return type(self) == type(other) and other.ids == self.ids

class ArbSellMMSwap(ArbStrat):
    '''
    MM swap bid, then sell call, and buy put
    '''

    @property
    def profit(self):
        return (self.forward.strike + self.forward.call.bid) - (self.swap.bid + self.forward.put.ask)

    @property
    def cost(self):
        return self.swap.bid + (0 if self.forward.call.bid > self.forward.put.ask else self.forward.put.ask - self.forward.call.bid)

    @property
    def initial_cost(self):
        return self.swap.bid

    @property
    def valid(self):
        return self.swap.bid != 0 and self.forward.call.bid != 0 and self.forward.put.ask != 0

    @property
    def ids(self):
        return { self.swap.id, self.forward.call.id, self.forward.put.id } 

    def __str__(self):
        return f'MM Swap {self.swap.id} -> Sell Call {self.forward.call.id} -> Buy Put {self.forward.put.id}'

    def create_order(self):
        class Order(StratOrder):
            STATE_WAIT_FOR_MATCH = 0
            STATE_WAIT_FOR_PROFIT = 1
            STATE_WAIT_FOR_CANCEL = 2

            def __init__(self, strat):
                self.strat = strat
                self.ids = self.strat.ids
                self.active_order = None
                self.current_strat = None
                self.state = self.STATE_NEW

            async def run(self):
                print(f'Placing order for swap on {self.strat.swap.id} for 1 @ {self.strat.swap.bid*100 + 100}')
                self.active_order = await self.strat.api.limit_buy(self.strat.swap.id, self.strat.swap.bid*100 + 100, 1)
                self.state = self.STATE_WAIT_FOR_MATCH
                self.price = self.strat.swap.bid + 1

            async def update(self, msg, msg_type):
                if msg_type == 'book_top':
                    if self.state == self.STATE_WAIT_FOR_MATCH and msg['contract_id'] in self.strat:
                        if self.strat.profit < 0 or self.strat.swap.bid > self.price or not self.strat.valid:
                            print('Cancelling order')
                            self.state = self.STATE_WAIT_FOR_CANCEL
                            await self.strat.api.cancel_order(self.active_order, self.strat.swap.id)
                        else:
                            print(f'Maintaining current strat {self.strat}. {self.strat.profit} us/them {self.price}/{self.strat.swap.bid}')

                elif msg_type == 'action_report':
                    # Here is where we'll process order update messages, such as fills or cancellations
                    if (self.state in [self.STATE_WAIT_FOR_MATCH, self.STATE_WAIT_FOR_CANCEL]) and self.active_order and msg['mid'] == self.active_order:
                        if msg['status_type'] == ledgerx.STATUS_TRADE:
                            print(f'Received fill for order {active_order}')
                            print(msg)
                            print('Stopping..')
                            print(f'Placing market sell order for 1 call {self.strat.forward.call.id}')
                            print(f'Placing market buy order for 1 put {self.strat.forward.put.id}')
                            await self.strat.api.market_sell(self.strat.forward.call.id, 1)
                            await self.strat.api.market_buy(self.strat.forward.put.id, 1)
                            self.state = self.STATE_DONE
                        elif msg['status_type'] == ledgerx.STATUS_CANCELLED:
                            print(f'Order cancelled.')
                            self.state = self.STATE_CANCELLED

            async def cancel(self):
                # After an attempted cancellation, the order should continue to run normally;
                # in action report, we'll set the correct state, depending on what happens
                await self.strat.api.cancel_order(self.active_order, self.strat.swap.id)

        return Order(self)

class ArbSellMMPut(ArbStrat):
    '''
    MM put bid, then buy swap, and sell call
    '''

    @property
    def profit(self):
        return (self.forward.strike + self.forward.call.ask) - (self.swap.ask + self.forward.put.bid)

    @property
    def cost(self):
        return self.forward.put.bid + self.swap.ask

    @property
    def initial_cost(self):
        return self.forward.put.bid

    # Maybe check here if our bid was outdone?
    @property
    def valid(self):
        return (self.forward.put.bid != 0 and self.swap.ask != 0 and self.forward.call.bid != 0)

    def __str__(self):
        return f'MM Put {self.forward.put.id} -> Buy Swap {self.swap.id} -> Sell Call {self.forward.call.id}'

    @property
    def ids(self):
        return { self.swap.id, self.forward.call.id, self.forward.put.id } 

    def create_order(self):
        class Order(StratOrder):
            STATE_WAIT_FOR_MATCH = 0
            STATE_WAIT_FOR_PROFIT = 1
            STATE_WAIT_FOR_CANCEL = 2

            def __init__(self, strat):
                self.strat = strat
                self.ids = self.strat.ids
                self.active_order = None
                self.current_strat = None
                self.price = 0
                self.state = self.STATE_NEW

            async def run(self):
                print(f'Placing order for put {self.strat.forward.put.id} for 1 @ {self.strat.forward.put.bid*100 + 100}')
                self.active_order = await self.strat.api.limit_buy(self.strat.forward.put.id, self.strat.forward.put.bid*100 + 100, 1)
                self.price = self.strat.forward.put.bid + 1
                self.state = self.STATE_WAIT_FOR_MATCH

            @property
            def should_cancel(self):
                return self.strat.profit < 0 or self.strat.forward.put.bid > self.price or not self.strat.valid

            async def update(self, msg, msg_type):
                if msg_type == 'book_top':
                    if self.state == self.STATE_WAIT_FOR_MATCH and msg['contract_id'] in self.strat and self.should_cancel:
                        print('Cancelling order')
                        # Potential problem in the future if the swap changes on us.. Should probably pair w/ order as metadata
                        self.state = self.STATE_WAIT_FOR_CANCEL
                        await self.strat.api.cancel_order(self.active_order, self.strat.forward.put.id)
                    else:
                        print(f'Maintaining current strat [{self.strat}]. {self.strat.profit} us/them {self.price}/{self.strat.forward.put.bid}')

                elif msg_type == 'action_report':
                    # Here is where we'll process order update messages, such as fills or cancellations
                    if (self.state in [self.STATE_WAIT_FOR_MATCH, self.STATE_WAIT_FOR_CANCEL]) and self.active_order and msg['mid'] == self.active_order:
                        if msg['status_type'] == ledgerx.STATUS_TRADE:
                            print(f'Received fill for order {active_order}')
                            print(msg)
                            print('Stopping..')
                            print(f'Placing market buy order for 1 swap {self.strat.swap.id}')
                            print(f'Placing market sell order for 1 call {self.strat.forward.call.id}')
                            await self.strat.api.market_buy(self.strat.swap.id, 1)
                            await self.strat.api.market_sell(self.strat.forward.call.id, 1)
                            self.state = self.STATE_DONE
                        if msg['status_type'] == ledgerx.STATUS_CANCELLED:
                            print('Order cancelled')
                            self.state = self.STATE_CANCELLED

            async def stop(self):
                # After an attempted cancellation, the order should continue to run normally;
                # in action report, we'll set the correct state, depending on what happens
                self.state = self.STATE_WAIT_FOR_CANCEL
                await self.strat.api.cancel_order(self.active_order, self.strat.swap.id)

        return Order(self)


