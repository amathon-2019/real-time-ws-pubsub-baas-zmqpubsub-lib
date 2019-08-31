import aioredis


class RedisContext:

    def __init__(self, address, loop=None):
        self.loop = loop
        self.address = address

    async def __aenter__(self):
        self.conn = await aioredis.create_redis(self.address, loop=self.loop)
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn and not self.conn.closed:
            self.conn.close()
            await self.conn.wait_closed()



