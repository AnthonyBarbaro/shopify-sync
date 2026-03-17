from collections import deque
from threading import Lock
from typing import Deque, List, Optional

from app.models import SyncResult


class SyncActivityStore:
    def __init__(self, *, limit: int = 200) -> None:
        self._events: Deque[SyncResult] = deque(maxlen=limit)
        self._lock = Lock()

    def record(self, result: SyncResult) -> None:
        with self._lock:
            self._events.appendleft(SyncResult(**result.dict()))

    def list(self, *, limit: int = 25, shop_domain: Optional[str] = None) -> List[SyncResult]:
        with self._lock:
            items = list(self._events)
            if shop_domain:
                items = [item for item in items if item.shop_domain == shop_domain]
            return [SyncResult(**item.dict()) for item in items[:limit]]

    def total(self, *, shop_domain: Optional[str] = None) -> int:
        with self._lock:
            if not shop_domain:
                return len(self._events)
            return sum(1 for item in self._events if item.shop_domain == shop_domain)
