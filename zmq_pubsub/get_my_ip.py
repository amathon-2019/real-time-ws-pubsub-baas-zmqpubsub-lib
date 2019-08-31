import socket

import aiohttp


async def get_my_ip():
    return socket.gethostbyname(socket.gethostname())
    # async with aiohttp.ClientSession() as session:
    #     async with session.get('https://api.ipify.org/') as response:
    #         return await response.text()


if __name__ == '__main__':
    import asyncio


    async def main():
        print(await get_my_ip())


    asyncio.get_event_loop().run_until_complete(main())
