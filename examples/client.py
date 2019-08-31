import asyncio

from zmq_pubsub import PubSubClient

if __name__ == '__main__':
    async def main():
        c = await PubSubClient.create('redis://127.0.0.1')
        c.subscribe('')

        async for event in c.read_iter():
            print(f'receive {event}')
            print(event.header)
            print(event.body)


    asyncio.get_event_loop().run_until_complete(main())
