from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from ml_playground.configuration.models import PreparerConfig
from ml_playground.experiments import protocol as exp_protocol


def test_side_effect_report_summarize_counts() -> None:
    report = exp_protocol.PrepareReport(
        created_files=(Path("created"),),
        updated_files=(Path("updated1"), Path("updated2")),
        skipped_files=(),
        messages=("done",),
    )
    summary = report.summarize()
    assert "created=1" in summary
    assert "updated=2" in summary
    assert "skipped=0" in summary


@runtime_checkable
class _RuntimePreparer(exp_protocol.Preparer, Protocol):
    pass


@runtime_checkable
class _RuntimeTrainer(exp_protocol.Trainer, Protocol):
    pass


@runtime_checkable
class _RuntimeSampler(exp_protocol.Sampler, Protocol):
    pass


def test_protocol_placeholders_execute_without_side_effects() -> None:
    preparer_placeholder = exp_protocol.Preparer.prepare
    trainer_placeholder = exp_protocol.Trainer.train
    sampler_placeholder = exp_protocol.Sampler.sample

    cfg = PreparerConfig()
    assert preparer_placeholder(object(), cfg) is None
    assert trainer_placeholder(object(), object()) is None
    assert sampler_placeholder(object(), object()) is None


def test_protocol_runtime_checks_accept_compliant_implementations() -> None:
    class ConcretePreparer:
        def prepare(self, cfg: PreparerConfig) -> exp_protocol.PrepareReport:
            del cfg
            return exp_protocol.PrepareReport()

    class ConcreteTrainer:
        def train(self, cfg):
            del cfg
            return exp_protocol.TrainReport()

    class ConcreteSampler:
        def sample(self, cfg):
            del cfg
            return exp_protocol.SampleReport()

    assert isinstance(ConcretePreparer(), _RuntimePreparer)
    assert isinstance(ConcreteTrainer(), _RuntimeTrainer)
    assert isinstance(ConcreteSampler(), _RuntimeSampler)


def test_experiment_integration_placeholders_execute() -> None:
    get_preparer = exp_protocol.ExperimentIntegration.get_preparer
    get_trainer = exp_protocol.ExperimentIntegration.get_trainer
    get_sampler = exp_protocol.ExperimentIntegration.get_sampler

    assert get_preparer(object()) is None
    assert get_trainer(object()) is None
    assert get_sampler(object()) is None
