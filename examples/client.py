import asyncio

from zmq_pubsub import PubSubClient

if __name__ == '__main__':
    async def main():
        client = await PubSubClient.create('redis://wsps-redis.xn--lg3bt3ss6d.com')

        client.subscribe('topic')
        await client.increase_client_cnt()
        try:
            async for msg in client.read_iter():
                print(msg)
                if msg.header == 'quit':
                    break
        finally:
            await client.decrease_client_cnt()


    asyncio.get_event_loop().run_until_complete(main())
