import asyncio

from zmq_pubsub import PubSubServer

if __name__ == '__main__':
    async def main():
        server = await PubSubServer.create('redis://127.0.0.1')
        asyncio.get_event_loop().create_task(server.run_forever())

        while True:
            await asyncio.sleep(1)
            await server.publish('topic', 'header', {'msg': f'hello'})
