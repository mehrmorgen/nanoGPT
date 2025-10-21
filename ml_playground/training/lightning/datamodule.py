"""Data module backed by the existing SimpleBatches sampler for Lightning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterator, Literal

import pytorch_lightning as pl
import torch
from torch.utils.data import DataLoader, IterableDataset

from ml_playground.configuration.models import SharedConfig, TrainerConfig
from ml_playground.data_pipeline.sampling.batches import SimpleBatches


__all__ = [
    "LightningBatchDataModule",
    "LightningDataDependencies",
]


@dataclass(frozen=True)
class LightningDataDependencies:
    """Dependency bag for Lightning data module construction."""

    initialize_batches: Callable[[TrainerConfig, SharedConfig], SimpleBatches]


def default_data_dependencies() -> LightningDataDependencies:
    """Provide default dependency wiring for the Lightning data module."""

    from ml_playground.training.hooks.data import initialize_batches

    return LightningDataDependencies(initialize_batches=initialize_batches)


class _BatchIterableDataset(IterableDataset[tuple[torch.Tensor, torch.Tensor]]):
    """Wrap a callable batch provider as an iterable dataset."""

    def __init__(
        self, provider: Callable[[], tuple[torch.Tensor, torch.Tensor]], length: int
    ) -> None:
        super().__init__()
        self._provider = provider
        self._length = max(int(length), 1)

    def __iter__(self) -> Iterator[tuple[torch.Tensor, torch.Tensor]]:
        for _ in range(self._length):
            yield self._provider()

    def __len__(self) -> int:  # pragma: no cover - lightning inspects this
        return self._length


class LightningBatchDataModule(pl.LightningDataModule):
    """Expose the SimpleBatches sampler as Lightning dataloaders."""

    def __init__(
        self,
        cfg: TrainerConfig,
        shared: SharedConfig,
        *,
        deps: LightningDataDependencies | None = None,
    ) -> None:
        super().__init__()
        self.cfg = cfg
        self.shared = shared
        self.deps = deps or default_data_dependencies()
        self._batches: SimpleBatches | None = None

    def setup(self, stage: str | None = None) -> None:  # pragma: no cover - lightning hook
        del stage
        if self._batches is None:
            self._batches = self.deps.initialize_batches(self.cfg, self.shared)

    def _length_for_split(self, split: Literal["train", "val"]) -> int:
        if split == "train":
            iters = self.cfg.runtime.iters_per_epoch or self.cfg.runtime.max_iters
        else:
            iters = self.cfg.runtime.eval_iters
        return max(int(iters), 1)

    def _dataset_for_split(self, split: Literal["train", "val"]) -> _BatchIterableDataset:
        length = self._length_for_split(split)

        def _provider() -> tuple[torch.Tensor, torch.Tensor]:
            assert self._batches is not None
            return self._batches.get_batch(split)

        return _BatchIterableDataset(_provider, length)

    def train_dataloader(self) -> DataLoader:
        dataset = self._dataset_for_split("train")
        return DataLoader(dataset, batch_size=None)

    def val_dataloader(self) -> DataLoader:
        dataset = self._dataset_for_split("val")
        return DataLoader(dataset, batch_size=None)
