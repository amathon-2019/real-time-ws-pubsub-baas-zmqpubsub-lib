import asyncio

from zmq_pubsub import PubSubClient

if __name__ == '__main__':
    async def main():
        client = await PubSubClient.create('redis://127.0.0.1')

        client.subscribe('topic')

        async for msg in client.read_iter():
            print(msg)


    asyncio.get_event_loop().run_until_complete(main())
