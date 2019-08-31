import random
from typing import Optional, AsyncGenerator

import aiozmq
import aioredis_lock

from .connection import Connection
from .event import Event


class PubSubClient(Connection):
    stream: Optional[aiozmq.ZmqStream] = None
    publisher_ip = None
    channel_name = ''
    is_increased = False

    @classmethod
    async def create(cls, redis_uri, *, loop=None):
        instance = cls(redis_uri, loop=loop)
        # await instance.set_redis()
        instance.closed = False
        # print(f'connect...')
        instance.stream = await aiozmq.stream.create_zmq_stream(
            zmq_type=instance.ZMQ_SUB,
            connect=f'tcp://127.0.0.1:{instance.CLIENT_PUB_PORT}'
        )

        return instance

    async def get_publisher_ip(self):
        # TODO: 랜덤이 아닌 균등하게 분포
        if self.publisher_ip is None:
            ips = list(await self.get_all_hosts_iter())
            self.publisher_ip = random.choice(ips)

        return self.publisher_ip

    def subscribe(self, channel: str):
        self.channel_name = channel
        if channel == '*':
            self.stream.transport.subscribe(b'')
        else:
            self.stream.transport.subscribe(channel.encode() + b'\0')

    async def read_iter(self) -> AsyncGenerator[Event, None]:
        while not self.closed:
            yield Event.from_bytes(await self.stream.read())

    async def _add_client_cnt(self, n):
        async with self.redis() as conn:
            async with aioredis_lock.RedisLock(conn, 'lock:{}:client_cnt'.format(self.channel_name)):
                key = '{}:client_cnt'.format(self.channel_name)
                value = await conn.get(key)
                value = int(value) if value else 0
                await conn.set(key, value + n)

    async def increase_client_cnt(self):
        if not self.is_increased:
            await self._add_client_cnt(1)
            self.is_increased = True

    async def decrease_client_cnt(self):
        if self.is_increased:
            await self._add_client_cnt(-1)
            self.is_increased = False
