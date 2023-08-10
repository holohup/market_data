import asyncio
import redis.asyncio as redis
import logging
import settings
from commands import SubscriptionRequest, Request


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG
)

request_channel = 'subscription_requests'
marketdata_channel = 'marketdata'
r = redis.from_url(settings.get('SUBSCRIPTIONS_URL'))
psub = r.pubsub()
pub = redis.from_url(settings.get('SUBSCRIPTIONS_URL'), decode_responses=True)

REQUESTS = {
    'subscribe': SubscriptionRequest
}


def parse_command(msg: str) -> Request:
    args = msg.split()
    command = args.pop(0)
    return REQUESTS[command](*args)


async def reader():
    async with psub as p:
        await p.subscribe(request_channel)
        if p is not None:
            while True:
                message = await p.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    message = message['data'].decode()
                    command = parse_command(message)
                    print(command)
                    await publish(message)


async def publish(msg):
    async with pub:
        await pub.publish(marketdata_channel, msg)


async def main():
    await reader()


if __name__ == '__main__':
    asyncio.run(main())
