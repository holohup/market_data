import asyncio
import redis.asyncio as redis
import logging
import settings
from commands import SubscriptionRequest, Request
import json
import watchers


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO
)

request_channel = 'subscription_requests'
marketdata_channel = 'marketdata'
r = redis.from_url(settings.get('SUBSCRIPTIONS_URL'))
psub = r.pubsub()
pub = redis.from_url(settings.get('SUBSCRIPTIONS_URL'), decode_responses=True)

REQUESTS = {'subscribe': SubscriptionRequest}


def parse_command(msg: str) -> Request:
    return json.loads(msg)
    args = msg.split()
    command = args.pop(0)
    return REQUESTS[command](*args)


async def reader():
    async with psub as p:
        active_watchers = {}
        await p.subscribe(request_channel)
        if p is not None:
            while True:
                message = await p.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    command = parse_command(message['data'].decode())
                    logging.info(f'Received subscription request: {command}')
                    broker = command['broker']
                    if broker not in active_watchers:
                        active_watchers[broker] = watchers.WATCHERS[broker](
                            publish=publish
                        )
                    handler = active_watchers[broker]
                    await handler.subscribe(
                        id_=command['id'], contract=command['contract']
                    )
                    logging.info(f"subscribed to {command['contract']}")


async def publish(msg):
    async with r:
        await r.publish(marketdata_channel, msg)


async def main():
    await reader()


if __name__ == '__main__':
    asyncio.run(main())
