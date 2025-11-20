from __future__ import annotations

from pathlib import Path

import torch

from ml_playground.training.types import (
    BatchProvider,
    OptimizerLike,
    ScaledLoss,
    TensorboardWriter,
    VectorizeFn,
)


class _DummyTensorboardWriter:
    def __init__(self) -> None:
        self.scalars: list[tuple[str, float, int | None]] = []
        self.closed = False

    def add_scalar(
        self,
        tag: str,
        scalar_value: float,
        global_step: int | None = None,
        *,
        walltime: float | None = None,
        new_style: bool = False,
        double_precision: bool = False,
    ) -> None:
        del walltime, new_style, double_precision
        self.scalars.append((tag, scalar_value, global_step))

    def close(self) -> None:  # pragma: no cover - trivial
        self.closed = True


class _DummyScaledLoss:
    def __init__(self) -> None:
        self.backward_called = False

    def backward(self) -> None:
        self.backward_called = True


class _DummyOptimizer(OptimizerLike):
    def __init__(self) -> None:
        self.param_groups = [{"lr": 0.1}]
        self._state: dict[str, object] = {"step": 0}
        self.zero_grad_called: list[bool] = []
        self.step_called = 0

    def state_dict(self) -> dict[str, object]:
        return dict(self._state)

    def load_state_dict(self, state_dict: dict[str, object]) -> None:
        self._state = dict(state_dict)

    def zero_grad(self, *, set_to_none: bool = True) -> None:
        self.zero_grad_called.append(set_to_none)

    def step(self) -> None:
        self.step_called += 1


class _DummyBatchProvider(BatchProvider):
    def __init__(self) -> None:
        self.train_calls = 0
        self.val_calls = 0

    def get_batch(self, split: str):  # type: ignore[override]
        x = torch.ones(2, 3)
        y = torch.zeros(2, 3)
        if split == "train":
            self.train_calls += 1
            return x, y
        if split == "val":
            self.val_calls += 1
            return x * 2, y + 1
        raise ValueError(f"unexpected split: {split}")


def test_tensorboard_writer_protocol_usage() -> None:
    writer: TensorboardWriter = _DummyTensorboardWriter()
    writer.add_scalar("loss", 0.1, global_step=1)
    writer.close()

    assert isinstance(writer, _DummyTensorboardWriter)
    assert writer.scalars == [("loss", 0.1, 1)]
    assert writer.closed is True


def test_scaled_loss_and_vectorize_fn_protocols() -> None:
    loss = _DummyScaledLoss()

    def fn(a: torch.Tensor, b: torch.Tensor) -> torch.Tensor:
        return (a + b).sum().unsqueeze(0)

    def vectorize(f):  # type: ignore[no-untyped-def]
        def wrapper(x: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
            out = f(x, y)
            loss.backward()
            return out

        return wrapper

    vec: VectorizeFn = vectorize
    wrapped = vec(fn)

    x = torch.ones(2, 2)
    y = torch.ones(2, 2)
    out = wrapped(x, y)

    assert out.shape == (1,)
    assert loss.backward_called is True


def test_optimizer_like_and_batch_provider_protocols(tmp_path: Path) -> None:
    opt: OptimizerLike = _DummyOptimizer()
    provider: BatchProvider = _DummyBatchProvider()

    # Simulate one training step using the protocols.
    x, y = provider.get_batch("train")
    assert x.shape == y.shape

    state_before = opt.state_dict()
    opt.zero_grad(set_to_none=False)
    opt.step()

    # Reload state and perform a validation batch to ensure both paths execute.
    opt.load_state_dict(state_before)
    x_val, y_val = provider.get_batch("val")

    assert x_val.shape == y_val.shape
    assert opt.zero_grad_called == [False]
    assert opt.step_called == 1
