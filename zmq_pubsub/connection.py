import asyncio
import time
from typing import Optional, Callable

import aioredis

from .redis_conn import RedisContext


class Connection:
    REDIS_HOSTS_KEY = 'zmq_host'
    my_ip = None
    redis: Callable[[], RedisContext] = None
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
        self.redis = self.make_redis_conn

    def make_redis_conn(self):
        return RedisContext(self.redis_uri, loop=self.loop)

    async def set_redis(self):
        # self.redis = self.make_redis_conn
        # self.redis_sub = RedisContext(self.redis_uri, loop=self.loop)
        pass

    async def redis_publish(self, channel, msg):
        async with self.redis() as conn:
            # print(f'redis publish {msg}')
            try:
                await conn.publish_json(channel, msg)
            except aioredis.errors.ConnectionForcedCloseError:
                pass

    async def redis_receive_iter(self, channel_name):
        async with self.redis() as conn:
            channel: aioredis.pubsub.Channel
            channel, *_ = await conn.subscribe(channel_name)
            while await channel.wait_message():
                msg = await channel.get_json()
                # print(f'redis receive: {msg}')
                yield msg

    async def update_status(self):
        async with self.redis() as conn:
            await conn.hset(self.REDIS_HOSTS_KEY, self.my_ip, int(time.time()))

    async def get_all_hosts_iter(self, interval=20):
        async with self.redis() as conn:
            data = await conn.hgetall(self.REDIS_HOSTS_KEY)
        min_time = time.time() - interval

        def _iter():
            for b_ip, b_time in data.items():
                ip = b_ip.decode()
                if ip != self.my_ip and min_time < int(b_time):
                    yield ip

        return _iter()

    async def get_channel_client_cnt(self, channel_name):
        key = '{}:client_cnt'.format(channel_name)
        async with self.redis() as conn:
            value = conn.get(key)

        return int(value) if value else 0

    async def get_channel_request_cnt(self, channel: str, interval=60):
        _t = time.time() - interval
        key = '{}:pub_cnt'.format(channel)
        async with self.redis() as conn:
            data = await conn.hgetall(key)

            n = 0
            rm_list = []

            for item in data.keys():
                _time, _ = item.split()
                if _t <= float(_time):
                    n += 1
                else:
                    rm_list.append(item)

            await asyncio.gather(*[conn.hdel(key, value) for value in rm_list])

        return n
