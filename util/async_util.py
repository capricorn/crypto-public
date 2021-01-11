import asyncio
import json

import websockets

# Create tasks, continue running until all of them have finished..?
# Actually, this should implement an async generator class
# Would be if we could make a class that overloads the '|' operator to
# merge streams together
class merge():
    def __init__(self, *agens):
        self.queue = asyncio.Queue()
        self.tasks = []
        self.agens = agens

    async def _queue_events(self, agen):
        async for event in agen:
            self.queue.put_nowait(event)
            #await asyncio.sleep(0)

    # This seems to be where we should set up our tasks..
    def __aiter__(self):
        self.tasks = [ asyncio.create_task(self._queue_events(gen)) for gen in self.agens ]
        return self

    # Needs to return an awaitable..
    async def __anext__(self):
        if all([ task.done() for task in self.tasks ]):
            raise StopAsyncIteration()
        else:
            return await self.queue.get()

async def test():
    ws1 = await websockets.connect('wss://ws-feed.pro.coinbase.com')
    ws2 = await websockets.connect('wss://ws.kraken.com')

    await ws1.send(json.dumps({
        'type': 'subscribe',
        'product_ids': [
            'BTC-USD'
        ],
        'channels': ['heartbeat']
    }))

    await ws2.send(json.dumps({
        'event': 'subscribe',
        'pair': [
            'XBT/USD'
        ],
        'subscription': {
            'name': 'ticker',
        }
    }))

    async for event in merge(ws1, ws2):
        print(event)

if __name__ == '__main__':
    asyncio.run(test())
