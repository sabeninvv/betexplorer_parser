from os import getenv
import asyncio

from redis import StrictRedis
from aiohttp import ClientSession, ClientTimeout, TCPConnector


def create_redis_connection(**kwargs):
    host = kwargs.get('host', getenv('REDIS_PASSWORD', "localhost"))
    port = int(kwargs.get('port', getenv('REDIS_PORT', "6379")))
    password = kwargs.get('password', getenv('REDIS_PASSWORD', None))
    return StrictRedis(host=host, port=port, password=password)


def create_aiohttp_session(**kwargs):
    async def create_aiohttp_session_():
        return ClientSession(
            connector=TCPConnector(force_close=True),
            trust_env=True,
            timeout=ClientTimeout(total=2 * 60 * 60, sock_read=480)
        )

    loop = kwargs.get('loop', asyncio.get_event_loop())
    return loop.run_until_complete(create_aiohttp_session_())
