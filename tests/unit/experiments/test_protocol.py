from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from ml_playground.configuration.models import PreparerConfig
from ml_playground.experiments import protocol as exp_protocol


def test_side_effect_report_summarize_counts() -> None:
    """Test side effect report summarize counts."""
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


class _ConcretePreparer:
    def prepare(self, cfg: PreparerConfig) -> exp_protocol.PrepareReport:
        del cfg
        return exp_protocol.PrepareReport()


class _ConcreteTrainer:
    def train(self, cfg: object) -> exp_protocol.TrainReport:
        del cfg
        return exp_protocol.TrainReport()


class _ConcreteSampler:
    def sample(self, cfg: object) -> exp_protocol.SampleReport:
        del cfg
        return exp_protocol.SampleReport()


class _ConcreteIntegration(exp_protocol.ExperimentIntegration):
    def get_preparer(self) -> exp_protocol.Preparer:
        return _ConcretePreparer()

    def get_trainer(self) -> exp_protocol.Trainer:
        return _ConcreteTrainer()

    def get_sampler(self) -> exp_protocol.Sampler:
        return _ConcreteSampler()


def test_protocol_placeholders_execute_without_side_effects() -> None:
    """Test protocol placeholders execute without side effects."""
    cfg = PreparerConfig()
    assert _ConcretePreparer().prepare(cfg) == exp_protocol.PrepareReport()
    assert _ConcreteTrainer().train(object()) == exp_protocol.TrainReport()
    assert _ConcreteSampler().sample(object()) == exp_protocol.SampleReport()


def test_protocol_runtime_checks_accept_compliant_implementations() -> None:
    """Test protocol runtime checks accept compliant implementations."""
    assert isinstance(_ConcretePreparer(), _RuntimePreparer)
    assert isinstance(_ConcreteTrainer(), _RuntimeTrainer)
    assert isinstance(_ConcreteSampler(), _RuntimeSampler)


def test_experiment_integration_placeholders_execute() -> None:
    """Test experiment integration placeholders execute."""
    integration = _ConcreteIntegration()
    assert isinstance(integration.get_preparer(), _RuntimePreparer)
    assert isinstance(integration.get_trainer(), _RuntimeTrainer)
    assert isinstance(integration.get_sampler(), _RuntimeSampler)
