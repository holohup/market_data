from abc import ABC, abstractmethod
import json
import ib_insync as ibi
import logging
import settings


ibi.util.logToConsole(logging.INFO)
ib_creds = (
    settings.get('IB_IP'),
    int(settings.get('IB_PORT')),
    int(settings.get('IB_ID')),
)


class IBWatcher:
    def __init__(self, publish: callable) -> None:
        self._publish = publish
        self._id_registry = {}
        self._ib = ibi.IB()
        self._ib.pendingTickersEvent += self._on_pending_tickers

    async def subscribe(self, id_: int, contract: dict) -> None:
        await self._publish(json.dumps({'request_received': id_}))
        if not self._ib.isConnected():
            await self._connect()
        filled_contract = self._prefill_contract(contract)
        c = self._create_contract(filled_contract)
        shortname = c.symbol + c.lastTradeDateOrContractMonth
        if shortname not in self._id_registry.keys():
            self._id_registry[shortname] = id_
            self._ib.reqMktData(c)

    def _on_pending_tickers(self, tickers):
        for t in tickers:
            print(
                t.contract.symbol + t.contract.lastTradeDateOrContractMonth,
                t.bid,
                t.ask,
            )

    async def _connect(self):
        await self._ib.connectAsync(*ib_creds)

    def _create_contract(self, contract):
        return ibi.Future(**contract)

    def _prefill_contract(self, contract):
        if 'exchange' not in contract.keys():
            contract['exchange'] = 'NYMEX'
        if 'currency' not in contract.keys():
            contract['currency'] = 'USD'
        return contract


# class TCSWatcher(Watcher):
#     pass


WATCHERS = {
    # 'tcs': TCSWatcher,
    'ib': IBWatcher
}
