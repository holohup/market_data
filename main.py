import asyncio
import json
import logging

import redis.asyncio as redis

import settings
import watchers
from commands import Request, SubscriptionRequest

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG
)

request_channel = 'subscription_requests'
marketdata_channel = 'marketdata'
r = redis.from_url(settings.get('SUBSCRIPTIONS_URL'), decode_responses=True)
psub = r.pubsub()

REQUESTS = {'subscribe': SubscriptionRequest}


def parse_command(msg: str) -> Request:
    return json.loads(msg)


async def reader():
    async with psub as p:
        active_watchers = {}
        await p.subscribe(request_channel)
        if p is not None:
            async for message in p.listen():
                if message['type'] != 'message':
                    continue
                command = parse_command(message['data'])
                logging.info(f'Received subscription request: {command}')
                broker = command['broker']
                if broker not in active_watchers:
                    active_watchers[broker] = watchers.WATCHERS[broker]
                handler = active_watchers[broker]
                handler.set_publish(publish)
                await handler.subscribe(
                    id_=command['id'], contract=command['contract']
                )
                await publish({'request_received': command['id']})
                logging.info(f"subscribed to {command['contract']}")


async def publish(msg):
    async with r:
        await r.publish(marketdata_channel, json.dumps(msg))


async def main():
    await reader()


if __name__ == '__main__':
    asyncio.run(main())
