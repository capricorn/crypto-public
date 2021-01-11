import asyncio

class PostOnlyException(Exception):
    pass

class UnknownOrderException(Exception):
    pass

class ExchangeAPI():
    def get_orderbook(self, pair):
        raise NotImplementedError()

class ExchangeWebsocket():
    def subscribe_orderbook_feed(self, pair):
        raise NotImplementedError()
