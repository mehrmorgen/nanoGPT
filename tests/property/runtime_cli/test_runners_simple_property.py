from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ml_playground.framework.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    MetadataConfig,
    TrainerConfig,
)
from ml_playground.runtime_cli import runners
from ml_playground.framework.runtime.core.bootstrap import CLIDependencies
from ml_playground.framework.runtime.core.results import ToolResult
from tests.support.config_builders import create_metadata_config


class _LoggerStub:
    def debug(self, msg: object, *args: object, **kwargs: object) -> None: ...

    def info(self, msg: object, *args: object, **kwargs: object) -> None: ...

    def warning(self, msg: object, *args: object, **kwargs: object) -> None: ...

    def error(self, msg: object, *args: object, **kwargs: object) -> None: ...


def _deps(result: ToolResult, tmp_path: Path) -> CLIDependencies:
    def _load(exp: str, cfg_path: Path | None) -> object:  # noqa: ARG001
        metadata = create_metadata_config(tmp_path)
        trainer_cfg = TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(
                device="cpu",
                dtype="float32",
                seed=0,
                out_dir=metadata.train_out_dir,
            ),
            logger=_LoggerStub(),
        )
        sampler_cfg = SamplerConfig(
            runtime=RuntimeConfig(
                device="cpu",
                dtype="float32",
                seed=0,
                out_dir=metadata.sample_out_dir,
            ),
            sample=SampleConfig(),
            logger=_LoggerStub(),
        )
        prepare_cfg = PreparerConfig(logger=_LoggerStub())

        return SimpleNamespace(
            prepare=prepare_cfg,
            training=trainer_cfg,
            sampling=sampler_cfg,
            metadata=metadata,
        )

    def _run_prepare(
        experiment: str,
        cfg: PreparerConfig,
        config_path: Path,
        metadata: MetadataConfig,
        deps: CLIDependencies,
        learning_engine: object | None = None,
    ) -> ToolResult:  # noqa: ARG001
        return result

    def _run_train(
        experiment: str,
        cfg: TrainerConfig,
        config_path: Path,
        metadata: MetadataConfig,
        deps: CLIDependencies,
        learning_engine: object | None = None,
    ) -> ToolResult:  # noqa: ARG001
        return result

    def _run_sample(
        experiment: str,
        cfg: SamplerConfig,
        config_path: Path,
        metadata: MetadataConfig,
        deps: CLIDependencies,
        learning_engine: object | None = None,
    ) -> ToolResult:  # noqa: ARG001
        return result

    return CLIDependencies(
        load_experiment=_load,
        ensure_train_prerequisites=lambda _: None,
        ensure_sample_prerequisites=lambda _: None,
        run_prepare=_run_prepare,
        run_train=_run_train,
        run_sample=_run_sample,
    )


@given(success=st.booleans())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_override_cli_dependencies_resets_after_context(
    tmp_path: Path, success: bool
) -> None:
    metadata = create_metadata_config(tmp_path)
    result = ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="ml",
        category="prepare",
        command="demo",
    )
    deps = _deps(result, tmp_path)

    with runners.override_cli_dependencies(deps):
        assert runners.get_cli_dependencies() is deps
        executed = runners.get_cli_dependencies().run_prepare(
            "demo",
            PreparerConfig(logger=_LoggerStub()),
            metadata.config_path,
            metadata,
            runners.get_cli_dependencies(),
            None,
        )
        assert executed is result

    # After context, dependency container is reset to default factory
    assert runners.get_cli_dependencies() is not deps


@given(success=st.booleans())
@settings(
    max_examples=5,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_reset_cli_dependencies_reverts_to_default(
    tmp_path: Path, success: bool
) -> None:
    metadata = create_metadata_config(tmp_path)
    result = ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="ml",
        category="train",
        command="demo",
    )

    def factory() -> CLIDependencies:
        return _deps(result, tmp_path)

    from ml_playground.framework.runtime.core import bootstrap as runtime_bootstrap

    original_factory = runtime_bootstrap.default_cli_dependencies
    try:
        runners.configure_cli_dependencies(factory)
        first = runners.get_cli_dependencies()

        runners.reset_cli_dependencies()
        second = runners.get_cli_dependencies()

        assert second is not first
        executed = second.run_train(
            "demo",
            TrainerConfig(
                model=ModelConfig(),
                data=DataConfig(),
                optim=OptimConfig(),
                schedule=LRSchedule(),
                runtime=RuntimeConfig(
                    device="cpu",
                    dtype="float32",
                    seed=0,
                    out_dir=metadata.train_out_dir,
                ),
                logger=_LoggerStub(),
            ),
            metadata.config_path,
            metadata,
            second,
            None,
        )
        assert isinstance(executed, ToolResult)
    finally:
        runners.configure_cli_dependencies(original_factory)
