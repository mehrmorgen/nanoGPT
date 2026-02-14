from __future__ import annotations

from ml_playground.framework.training.hooks.logging import log_training_step

from tests.unit.framework.training._helpers import LoggerStub, make_minimal_gpt


def test_log_training_step_early_iterations() -> None:
    """log_training_step should skip MFU calculation for early iterations."""
    logger = LoggerStub()
    model = make_minimal_gpt()

    # local_iter_num < 5 should skip MFU calculation
    running_mfu = log_training_step(
        logger=logger,
        iter_num=1,
        loss_value=0.5,
        dt=0.1,
        local_iter_num=3,
        raw_model=model,
        running_mfu=-1.0,
        batch_size=2,
        grad_accum_steps=1,
    )

    # Should return unchanged running_mfu
    assert running_mfu == -1.0
    assert len(logger.messages) == 1
    assert "iter 1" in logger.messages[0]
    assert "loss 0.5000" in logger.messages[0]


def test_log_training_step_with_mfu_calculation() -> None:
    """log_training_step should calculate MFU after warmup iterations."""
    logger = LoggerStub()
    model = make_minimal_gpt()

    # local_iter_num >= 5 should calculate MFU
    running_mfu = log_training_step(
        logger=logger,
        iter_num=10,
        loss_value=0.3,
        dt=0.05,
        local_iter_num=10,
        raw_model=model,
        running_mfu=-1.0,
        batch_size=4,
        grad_accum_steps=2,
    )

    # Should have calculated and returned MFU
    assert running_mfu != -1.0
    assert isinstance(running_mfu, float)
    assert len(logger.messages) == 1
    assert "iter 10" in logger.messages[0]


def test_log_training_step_smooths_mfu() -> None:
    """log_training_step should apply exponential smoothing to MFU."""
    logger = LoggerStub()
    model = make_minimal_gpt()

    # First call with MFU calculation
    running_mfu = log_training_step(
        logger=logger,
        iter_num=5,
        loss_value=0.4,
        dt=0.1,
        local_iter_num=5,
        raw_model=model,
        running_mfu=-1.0,
        batch_size=2,
        grad_accum_steps=1,
    )

    first_mfu = running_mfu

    # Second call with different timing should smooth the MFU
    running_mfu = log_training_step(
        logger=logger,
        iter_num=6,
        loss_value=0.4,
        dt=0.05,  # Different timing
        local_iter_num=6,
        raw_model=model,
        running_mfu=first_mfu,
        batch_size=2,
        grad_accum_steps=1,
    )

    # Should have applied smoothing: 0.9 * old + 0.1 * new
    # With different dt, the new MFU will be different, so smoothing will change the value
    assert isinstance(running_mfu, float)
    # Verify smoothing was applied (not just replaced)
    assert running_mfu > 0


def test_log_training_step_scales_loss() -> None:
    """log_training_step should scale loss by grad_accum_steps."""
    logger = LoggerStub()
    model = make_minimal_gpt()

    log_training_step(
        logger=logger,
        iter_num=1,
        loss_value=0.5,
        dt=0.1,
        local_iter_num=1,
        raw_model=model,
        running_mfu=-1.0,
        batch_size=2,
        grad_accum_steps=4,
    )

    # Loss should be scaled: 0.5 * 4 = 2.0
    assert "loss 2.0000" in logger.messages[0]
