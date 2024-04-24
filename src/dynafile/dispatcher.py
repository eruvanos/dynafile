from typing import NamedTuple, Optional, Callable, NoReturn, List


class Event(NamedTuple):
    action: str
    new: Optional[dict]
    old: Optional[dict]


EventListener = Callable[[Event], NoReturn]


class Dispatcher:
    def __init__(self):
        self._listeners: List[EventListener] = []

    def emit(self, event: Event):
        for listener in self._listeners:
            listener(event)

    def connect(self, listener: EventListener):
        self._listeners.append(listener)

    def remove(self, listener: EventListener):
        self._listeners.remove(listener)
