import asyncio

import ib_insync as ibi


class App:
    async def run(self):
        self.ib = ibi.IB()
        with await self.ib.connectAsync('192.168.2.40', 7496, 5):
            contracts = [
                ibi.Future(
                    symbol='QG',
                    lastTradeDateOrContractMonth='202309',
                    exchange='NYMEX',
                    currency='USD',
                ),
                ibi.Future(
                    symbol='QG',
                    lastTradeDateOrContractMonth='202310',
                    exchange='NYMEX',
                    currency='USD',
                ),
            ]
            for contract in contracts:
                self.ib.reqMktData(contract)

            async for tickers in self.ib.pendingTickersEvent:
                for t in tickers:
                    print(t.bid, t.ask, t.contract.symbol+t.contract.lastTradeDateOrContractMonth)
                self.ib.cancelMktData(contracts[1])
                print('unsubscribed')

    def stop(self):
        self.ib.disconnect()


app = App()
try:
    asyncio.run(app.run())
except (KeyboardInterrupt, SystemExit):
    app.stop()
