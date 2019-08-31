import asyncio
import random
import time
from typing import List, Optional

import aiozmq
from .connection import Connection
from .event import Event


class PubSubClient(Connection):
    stream: Optional[aiozmq.ZmqStream] = None
    publisher_ip = None

    @classmethod
    async def create(cls, redis_uri, *, loop=None):
        instance = cls(redis_uri, loop=loop)
        await instance.set_redis()
        instance.closed = False
        publisher_ip = await instance.get_publisher_ip()
        print(f'connect... {publisher_ip}')
        instance.stream = await aiozmq.stream.create_zmq_stream(
            zmq_type=instance.ZMQ_SUB,
            connect=f'tcp://{publisher_ip}:{instance.CLIENT_PUB_PORT}'
        )
        return instance

    async def get_publisher_ip(self):
        # TODO: 랜덤이 아닌 균등하게 분포
        if self.publisher_ip is None:
            ips = list(await self.get_all_hosts_iter())
            self.publisher_ip = random.choice(ips)

        return self.publisher_ip

    def subscribe(self, channel: str):
        self.stream.transport.subscribe(channel.encode())

    async def read_iter(self):
        while not self.closed:
            yield Event.deserialize(await self.stream.read())
