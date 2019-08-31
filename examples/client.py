import asyncio

from zmq_pubsub import PubSubClient

if __name__ == '__main__':
    async def main():
        c = await PubSubClient.create('redis://127.0.0.1')
        c.subscribe(b'')

        async for msg in c.read_iter():
            print(f'receive {msg}')


    asyncio.get_event_loop().run_until_complete(main())
