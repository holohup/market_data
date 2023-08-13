import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

import ib_insync as ibi
from grpc import StatusCode
from tinkoff.invest import (MarketDataResponse, OrderBookInstrument,
                            RequestError)
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings
from tinkoff.invest.services import MarketDataStreamManager
from tinkoff.invest.utils import quotation_to_decimal

import repo
import settings


@dataclass
class OrderBook:
    bid: float
    ask: float


ibi.util.logToConsole(logging.WARNING)
ib_creds = (
    settings.get('IB_IP'),
    int(settings.get('IB_PORT')),
    int(settings.get('IB_ID')),
)
tcs_ro_token = settings.get('TCS_RO_TOKEN')


class Watcher(ABC):
    def __init__(self) -> None:
        self._publish = None
        self._id_registry = {}
        self._orderbooks = {}
        self._post_init()
        super().__init__()

    def set_publish(self, publish: callable):
        self._publish = publish

    async def _publish_orderbook(self, id_):
        await self._publish(
            {
                'id': self._id_registry[id_],
                'bid': self._orderbooks[id_].bid,
                'ask': self._orderbooks[id_].ask,
            }
        )

    async def _update_orderbook(self, id_, bid, ask):
        if not isinstance(ask, float) or not isinstance(bid, float):
            return
        if id_ in self._id_registry.keys():
            if id_ not in self._orderbooks.keys():
                self._orderbooks[id_] = OrderBook(bid, ask)
            if self._orderbooks[id_] == OrderBook(bid, ask):
                return
            self._orderbooks[id_] = OrderBook(bid, ask)
            await self._publish_orderbook(id_)

    async def _publish_orderbooks_errors(self):
        for id_ in self._orderbooks.keys():
            self._orderbooks[id_] = OrderBook(-1, -1)
            await self._publish_orderbook(id_)

    @abstractmethod
    def _generate_internal_id(self, contract):
        pass

    @abstractmethod
    def _post_init(self):
        pass

    @abstractmethod
    def _create_contract(self):
        pass

    @abstractmethod
    async def _error_handler(self):
        pass


class IBWatcher(Watcher):
    def _post_init(self):
        self._ib = ibi.IB()
        ibi.util.patchAsyncio()
        self._ib.errorEvent += self._error_handler
        self._ib.pendingTickersEvent += self._on_pending_tickers
        self._connect()

    async def subscribe(self, id_: int, contract: dict) -> None:
        if not self._ib.isConnected():
            self._connect()
        c = self._create_contract(contract)
        shortname = self._generate_internal_id(c)
        if shortname not in self._id_registry.keys():
            self._id_registry[shortname] = id_
            self._ib.reqMktData(c)

    async def _on_pending_tickers(self, tickers):
        for t in tickers:
            id_ = self._generate_internal_id(t.contract)
            bid = t.bid
            ask = t.ask
            await self._update_orderbook(id_, bid, ask)

    async def _error_handler(self, reqId, errorCode, errorString, contract):
        if errorCode in (2103, 2108, 2110, 1100, 504, 502):
            await self._publish_orderbooks_errors()

    def _connect(self):
        self._ib.connect(*ib_creds)
        self._ib.reqMarketDataType(1)

    def _create_contract(self, contract):
        if 'exchange' not in contract.keys():
            contract['exchange'] = 'NYMEX'
        if 'currency' not in contract.keys():
            contract['currency'] = 'USD'

        return ibi.Future(**contract)

    def _generate_internal_id(self, contract):
        return contract.symbol + contract.lastTradeDateOrContractMonth


class TCSWatcher(Watcher):
    def _post_init(self):
        self._tcs_repo = repo.TCSAssetRepo()
        self._market_data_stream: MarketDataStreamManager = None
        asyncio.get_event_loop().create_task(self._run_subscriptions())

    async def subscribe(self, id_: int, contract: dict) -> None:
        c = self._create_contract(contract)
        shortname = self._generate_internal_id(c)
        if shortname not in self._id_registry.keys():
            self._id_registry[shortname] = id_
            self._subscribe_to_a_single_ticker(shortname)

    async def _run_subscriptions(self):
        async with AsyncRetryingClient(
            tcs_ro_token, RetryClientSettings()
        ) as client:
            while True:
                self._relaunch_stream(client)
                self._subscribe_to_all_tickers()
                try:
                    await self._receive_orderbooks()
                except RequestError as e:
                    await self._error_handler(e)
                except Exception as e:
                    logging.error(f'Unexpected general error: {e}')

    async def _receive_orderbooks(self):
        async for md in self._market_data_stream:
            ob: MarketDataResponse.orderbook = md.orderbook
            if ob is not None:
                id_ = ob.instrument_uid
                bid = float(quotation_to_decimal(ob.bids[0].price))
                ask = float(quotation_to_decimal(ob.asks[0].price))
                await self._update_orderbook(id_, bid, ask)

    async def _error_handler(self, e):
        if e.code == StatusCode.UNAVAILABLE or e.code == StatusCode.UNKNOWN:
            retry_time = 60
            logging.error(f'Stream unavailable, {e.code=}, waiting for 60 sec')
        elif e.code == StatusCode.CANCELLED:
            retry_time = e.metadata.ratelimit_reset
            logging.error(f'Stream cancelled, waiting for {retry_time} sec')
        else:
            retry_time = 60
            logging.error(f'Unexpected error {e}, waiting {retry_time} secs')

        await self._publish_orderbook_errors()
        await asyncio.sleep(retry_time)

    def _relaunch_stream(self, client):
        if self._market_data_stream:
            logging.info('Stopping TCS Stream')
            self._market_data_stream.stop()
            self._market_data_stream = None
        logging.info('Creating a new TCS Stream')
        self._market_data_stream = client.create_market_data_stream()

    def _subscribe_to_all_tickers(self):
        logging.info('Subscribing for new instruments in TCS')
        self._market_data_stream.order_book.subscribe(
            instruments=[
                OrderBookInstrument(instrument_id=id_, depth=1)
                for id_ in self._id_registry.keys()
            ]
        )

    def _subscribe_to_a_single_ticker(self, uid):
        self._market_data_stream.order_book.subscribe(
            instruments=[OrderBookInstrument(instrument_id=uid, depth=1)]
        )

    def _create_contract(self, contract):
        return self._tcs_repo[contract['ticker']]

    def _generate_internal_id(self, contract):
        return contract.uid


ib_watcher = IBWatcher()
tcs_watcher = TCSWatcher()

WATCHERS = {'tcs': tcs_watcher, 'ib': ib_watcher}
