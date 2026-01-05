"""Property-based tests for log_training_step behavior and branches."""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import example, given, settings, strategies as st

from ml_playground.training.hooks.logging import log_training_step


class _FakeLogger:
    def __init__(self, lr: float = 0.0) -> None:
        self.lr = lr
        self.infos: list[str] = []
        self.debugs: list[str] = []

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.infos.append(msg)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.debugs.append(msg)


class _FakeWriter:
    def __init__(self, explode: bool = False) -> None:
        self.explode = explode
        self.add_scalar_calls: list[tuple[str, float, int]] = []

    def add_scalar(self, tag: str, value: float, step: int) -> None:
        if self.explode:
            raise RuntimeError("writer failed")
        self.add_scalar_calls.append((tag, value, step))

    def close(self) -> None:  # pragma: no cover - not used here
        pass


class _FakeModel:
    def __init__(self, mfu: float) -> None:
        self._mfu = mfu

    def estimate_mfu(self, *_: Any) -> float:
        return self._mfu


@settings(max_examples=12, deadline=100, derandomize=True)
@given(
    loss=st.floats(min_value=0.0, max_value=10.0),
    dt=st.floats(min_value=0.001, max_value=1.0),
    running_mfu=st.floats(min_value=-1.0, max_value=200.0),
    mfu=st.floats(min_value=0.0, max_value=150.0),
    iter_num=st.integers(min_value=0, max_value=5),
    local_iter_num=st.integers(min_value=0, max_value=6),
)
@example(loss=1.0, dt=0.1, running_mfu=-1.0, mfu=120.0, iter_num=5, local_iter_num=5)
def test_log_training_step_updates_and_logs(
    loss: float,
    dt: float,
    running_mfu: float,
    mfu: float,
    iter_num: int,
    local_iter_num: int,
) -> None:
    """log_training_step logs info and updates running MFU when eligible."""

    logger = _FakeLogger(lr=0.5)
    writer = _FakeWriter(explode=False)
    model = _FakeModel(mfu=mfu)

    updated = log_training_step(
        logger=logger,
        iter_num=iter_num,
        loss_value=loss,
        dt=dt,
        local_iter_num=local_iter_num,
        raw_model=model,
        running_mfu=running_mfu,
        batch_size=2,
        grad_accum_steps=2,
        writer=writer,
        update_mode="log",
    )

    assert logger.infos  # info logged
    if local_iter_num >= 5:
        expected = (
            mfu
            if running_mfu == -1.0
            else pytest.approx(0.9 * running_mfu + 0.1 * float(mfu))
        )
        assert updated == expected
    else:
        assert updated == running_mfu
    assert writer.add_scalar_calls == [
        ("Loss/train", loss * 2, iter_num),
        ("LR", 0.5, iter_num),
    ]


def test_log_training_step_writer_disabled() -> None:
    """No writer means only info log and unchanged running MFU."""

    logger = _FakeLogger(lr=1.0)
    model = _FakeModel(mfu=10.0)

    updated = log_training_step(
        logger=logger,
        iter_num=1,
        loss_value=2.0,
        dt=0.1,
        local_iter_num=1,
        raw_model=model,
        running_mfu=5.0,
        batch_size=1,
        grad_accum_steps=1,
        writer=None,
        update_mode="eval",
    )

    assert logger.infos
    assert updated == 5.0


def test_log_training_step_writer_error_is_swallowed() -> None:
    """Writer errors are logged at debug and do not raise."""

    logger = _FakeLogger(lr=0.1)
    writer = _FakeWriter(explode=True)
    model = _FakeModel(mfu=1.0)

    updated = log_training_step(
        logger=logger,
        iter_num=2,
        loss_value=1.5,
        dt=0.2,
        local_iter_num=6,
        raw_model=model,
        running_mfu=-1.0,
        batch_size=1,
        grad_accum_steps=1,
        writer=writer,
        update_mode="log",
    )

    assert logger.infos
    assert logger.debugs  # writer failure recorded
    # running mfu updated because local_iter_num >= 5
    assert updated == 1.0


def test_log_training_step_skip_when_mode_not_log() -> None:
    """update_mode other than 'log' skips writer calls."""

    logger = _FakeLogger(lr=0.2)
    writer = _FakeWriter(explode=False)
    model = _FakeModel(mfu=2.0)

    updated = log_training_step(
        logger=logger,
        iter_num=3,
        loss_value=1.0,
        dt=0.1,
        local_iter_num=4,
        raw_model=model,
        running_mfu=0.0,
        batch_size=2,
        grad_accum_steps=1,
        writer=writer,
        update_mode="eval",
    )

    assert logger.infos
    assert not writer.add_scalar_calls
    assert updated == 0.0
