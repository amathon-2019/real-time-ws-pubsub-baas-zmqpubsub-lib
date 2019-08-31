import asyncio
import time
from typing import Optional, List

import aiozmq

from .get_my_ip import get_my_ip
from .connection import Connection
from .event import Event


class PubSubServer(Connection):
    server_stream: Optional[aiozmq.ZmqStream] = None
    client_stream: Optional[aiozmq.ZmqStream] = None

    def __init__(self, redis_uri, loop=None):
        super(self.__class__, self).__init__(redis_uri, loop)
        self.sub_streams: List[aiozmq.ZmqStream] = []

    @classmethod
    async def create(cls, redis_uri, *, loop=None):
        instance = cls(redis_uri, loop=loop)
        await instance.set_redis()
        instance.my_ip = await get_my_ip()
        await instance.update_status()

        instance.closed = False
        instance.server_stream, instance.client_stream = await asyncio.gather(
            aiozmq.stream.create_zmq_stream(
                zmq_type=PubSubServer.ZMQ_PUB,
                bind=f'tcp://0.0.0.0:{PubSubServer.SERVER_SUB_PORT}'
            ),
            aiozmq.stream.create_zmq_stream(
                zmq_type=PubSubServer.ZMQ_PUB,
                bind=f'tcp://0.0.0.0:{PubSubServer.CLIENT_PUB_PORT}'
            )
        )
        return instance

    async def close(self):
        self.closed = True

    async def update_forever(self, interval=5):
        while not self.closed:
            await asyncio.sleep(interval)
            await self.update_status()

    async def connect_server_sub(self, ip):
        stream = await aiozmq.stream.create_zmq_stream(
            zmq_type=self.ZMQ_SUB,
            connect=f'tcp://{ip}:{self.SERVER_SUB_PORT}',
        )
        stream.transport.subscribe(b'')
        self.sub_streams.append(stream)

    async def subscribe_other_servers(self):
        jobs = [self.connect_server_sub(ip) for ip in (await self.get_all_hosts_iter())]
        await asyncio.gather(*jobs)

    async def redirect_event_forever(self, sub_stream: aiozmq.ZmqStream):
        while not self.closed:
            event = await sub_stream.read()
            await self.client_stream.write(event)

    async def publish(self, event: Event):
        msg = event.serialize()

        def send_servers():
            if self.server_stream:
                self.server_stream.write(msg)

        def send_clients():
            if self.client_stream:
                print(f'send... {event}')
                self.client_stream.write(msg)

        send_servers()
        send_clients()

    async def run_forever(self):
        tasks = [self.redirect_event_forever(stream) for stream in self.sub_streams]
        tasks.append(self.update_forever())
        await asyncio.gather(*tasks)
