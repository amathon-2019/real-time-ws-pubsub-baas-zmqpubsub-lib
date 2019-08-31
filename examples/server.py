import asyncio

from zmq_pubsub import PubSubServer, Event

if __name__ == '__main__':
    async def main():
        s = await PubSubServer.create('redis://127.0.0.1')
        asyncio.get_event_loop().create_task(s.run_forever())

        while True:
            await asyncio.sleep(1)
            await s.publish('topic', 'header', {'msg': 'hello'})


    asyncio.get_event_loop().run_until_complete(main())
