import asyncio
import time
from typing import Optional

import aioredis

from .redis_conn import RedisContext


class Connection:
    REDIS_HOSTS_KEY = 'zmq_host'
    my_ip = None
    redis: Optional[RedisContext] = None
    redis_sub: Optional[RedisContext] = None
    SERVER_SUB_PORT = 10021
    CLIENT_PUB_PORT = 10022
    ZMQ_PUB = 1
    ZMQ_SUB = 2
    closed = True

    def __init__(self, redis_uri, loop=None):
        self.redis_uri = redis_uri
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

    async def set_redis(self):
        self.redis = RedisContext(self.redis_uri, loop=self.loop)
        self.redis_sub = RedisContext(self.redis_uri, loop=self.loop)

    async def redis_publish(self, channel, msg):
        async with self.redis as conn:
            # print(f'redis publish {msg}')
            try:
                await conn.publish_json(channel, msg)
            except aioredis.errors.ConnectionForcedCloseError:
                pass

    async def redis_receive_iter(self, channel_name):
        async with self.redis_sub as conn:
            channel: aioredis.pubsub.Channel
            channel, *_ = await conn.subscribe(channel_name)
            while await channel.wait_message():
                msg = await channel.get_json()
                # print(f'redis receive: {msg}')
                yield msg

    async def update_status(self):
        async with self.redis as conn:
            await conn.hset(self.REDIS_HOSTS_KEY, self.my_ip, int(time.time()))

    async def get_all_hosts_iter(self, interval=20):
        async with self.redis as conn:
            data = await conn.hgetall(self.REDIS_HOSTS_KEY)
        min_time = time.time() - interval

        def _iter():
            for b_ip, b_time in data.items():
                ip = b_ip.decode()
                if ip != self.my_ip and min_time < int(b_time):
                    yield ip

        return _iter()
