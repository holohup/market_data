from dataclasses import dataclass
from typing import Any


class Request:
    pass


@dataclass(frozen=True)
class SubscriptionRequest(Request):
    broker: str
    id: int
    contract: Any
