import asyncio
import redis.asyncio as redis
import logging
import settings
from json import dumps

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO
)

request_channel = 'subscription_requests'
marketdata_channel = 'marketdata'
r = redis.from_url(settings.get('SUBSCRIPTIONS_URL'))
psub = r.pubsub()
pub = redis.from_url(settings.get('SUBSCRIPTIONS_URL'), decode_responses=True)


async def reader():
    async with psub as p:
        await p.subscribe(marketdata_channel)
        if p is not None:
            while True:
                message = await p.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    message = message['data'].decode()
                    print(f'message received: {message}')
                    # await publish(message)


async def publish(msg, r):
    await r.publish(request_channel, msg)
