from __future__ import annotations

import hypothesis.strategies as st
from hypothesis import given

from ml_playground.configuration.models import ModelConfig
from ml_playground.models.core.model import GPT
from ml_playground.training.hooks.logging import log_training_step


class MockLogger:
    def __init__(self):
        self.infos = []
        self.debugs = []

    def info(self, msg: str):
        self.infos.append(msg)

    def debug(self, msg: str):
        self.debugs.append(msg)


@given(
    loss=st.floats(min_value=0.0, max_value=10.0),
    dt=st.floats(min_value=0.001, max_value=1.0),
    running_mfu=st.floats(min_value=-1.0, max_value=100.0),
    mfu=st.floats(min_value=0.0, max_value=150.0),
    iter_num=st.integers(min_value=0, max_value=1000000),
    local_iter_num=st.integers(min_value=0, max_value=1000),
)
def test_log_training_step_updates_and_logs(
    loss: float,
    dt: float,
    running_mfu: float,
    mfu: float,
    iter_num: int,
    local_iter_num: int,
) -> None:
    logger = MockLogger()
    model_cfg = ModelConfig(
        n_layer=1, n_head=1, n_embd=32, block_size=16, vocab_size=100
    )
    model = GPT(model_cfg, logger=None)

    # Patch estimate_mfu to return predictable value
    model.estimate_mfu = lambda *args, **kwargs: mfu

    new_mfu = log_training_step(
        logger=logger,
        iter_num=iter_num,
        loss_value=loss,
        dt=dt,
        local_iter_num=local_iter_num,
        raw_model=model,
        running_mfu=running_mfu,
        batch_size=12,
        grad_accum_steps=40,
    )

    assert len(logger.infos) == 1
    assert f"iter {iter_num}" in logger.infos[0]

    if local_iter_num < 5:
        assert new_mfu == running_mfu
    else:
        # Expected calculation: 0.9 * old + 0.1 * new
        expected = mfu if running_mfu == -1.0 else 0.9 * running_mfu + 0.1 * float(mfu)
        assert abs(new_mfu - expected) < 1e-6
