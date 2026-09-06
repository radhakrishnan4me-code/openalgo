"""Consume an async provider stream on the agent's real producer thread."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any


def iter_async_stream(make_stream: Callable[[], AsyncIterator[Any]]) -> Iterator[Any]:
    """Bridge an async stream to the synchronous SSE producer.

    Call only on a real OS thread, never on the eventlet request greenlet.
    ChatGPT's synchronous Responses bridge drives a synchronous iterator from
    inside an async loop; its per-chunk async hook then tries to nest a second
    loop. Consuming the async provider path avoids that conversion entirely.
    One loop owns the whole stream, including generator cleanup on disconnect
    or failure. No executor or additional thread is created here.
    """
    with asyncio.Runner() as runner:
        stream = make_stream()
        try:
            while True:
                try:
                    event = runner.run(anext(stream))
                except StopAsyncIteration:
                    return
                yield event
        finally:
            runner.run(stream.aclose())
