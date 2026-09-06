"""Async ChatGPT streams keep one loop and close it on every exit path."""

import asyncio
from types import SimpleNamespace

import pytest

from services.agent.async_stream import iter_async_stream


@pytest.mark.parametrize("ending", ["complete", "error", "disconnect"])
def test_stream_cleanup(ending):
    loops, closed = [], []

    async def events():
        loops.append(asyncio.get_running_loop())
        try:
            yield "first"
            await asyncio.sleep(0)
            assert asyncio.get_running_loop() is loops[0]
            if ending == "error":
                raise RuntimeError("provider failed")
            yield "second"
        finally:
            await asyncio.sleep(0)
            closed.append(True)

    stream = iter_async_stream(events)
    assert next(stream) == "first"
    if ending == "disconnect":
        stream.close()
    elif ending == "error":
        with pytest.raises(RuntimeError, match="provider failed"):
            list(stream)
    else:
        assert list(stream) == ["second"]
    assert closed == [True]
    assert loops[0].is_closed()


def test_coroutine_runtime_error_is_not_retried():
    calls = []

    async def events():
        calls.append(True)
        raise RuntimeError("Cannot run the event loop while another loop is running")
        yield  # make this an async generator

    with pytest.raises(RuntimeError, match="Cannot run"):
        list(iter_async_stream(events))
    assert calls == [True]


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("model", ["chatgpt/gpt-5.4", "openai/gpt-5.4"])
def test_stream_dispatch(monkeypatch, resume, model):
    pytest.importorskip("agno")
    from services.agent import stream

    calls = []

    class Agent:
        def run(self, *args, **kwargs):
            calls.append(("sync", args, kwargs))
            yield "event"

        continue_run = run

        async def arun(self, *args, **kwargs):
            calls.append(("async", args, kwargs))
            yield "event"

        acontinue_run = arun

    agent = Agent()
    agent.model = SimpleNamespace(id=model)
    monkeypatch.setattr(stream, "_pump", lambda agent, start, translator, **kw: list(start()))
    if resume:
        result = stream.stream_continue(
            agent,
            run_id="run",
            session_id="session",
            conversation_id=1,
            user_id="user",
            requirements=["approved"],
        )
        assert calls[0][2]["requirements"] == ["approved"]
        assert calls[0][2]["run_id"] == "run"
    else:
        result = stream.stream_run(
            agent, "hello", session_id="session", conversation_id=1, user_id="user"
        )
        assert calls[0][1] == ("hello",)
    assert result == ["event"]
    assert calls[0][0] == ("async" if model.startswith("chatgpt/") else "sync")
    assert calls[0][2]["stream"] is True
    assert calls[0][2]["stream_events"] is True
    assert calls[0][2]["session_id"] == "session"
    assert calls[0][2]["user_id"] == "user"


def test_async_stream_on_real_thread_under_eventlet():
    pytest.importorskip("eventlet")
    import subprocess
    import sys
    import textwrap

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent("""
            import eventlet
            eventlet.monkey_patch()
            import asyncio
            from services.agent.async_stream import iter_async_stream
            from utils.real_threading import Thread, join
            values, errors, loops = [], [], []
            async def events():
                loops.append(asyncio.get_running_loop())
                for i in range(3):
                    await asyncio.sleep(0.05)
                    yield i
            def produce():
                try:
                    values.extend(iter_async_stream(events))
                except BaseException as exc:
                    errors.append(exc)
            worker = Thread(target=produce)
            worker.start()
            ticks = 0
            while worker.is_alive():
                eventlet.sleep(0.01)
                ticks += 1
                assert ticks < 1000
            assert join(worker, timeout=1)
            assert not errors, errors
            assert values == [0, 1, 2], values
            assert ticks >= 5, ticks
            assert loops[0].is_closed()
        """),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
