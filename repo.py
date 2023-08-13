from pickle import loads

import redis
from tinkoff.invest.schemas import Bond, Currency, Etf, Future, Share

import settings

R = redis.Redis.from_url(settings.get('TCS_ASSETS_URL'))


class TCSAssetRepo:
    def __getitem__(self, id_: str) -> Bond | Currency | Etf | Future | Share:
        pickled_asset = R.get(id_.upper())
        R.close()
        if not pickled_asset:
            return None
        return loads(pickled_asset)
