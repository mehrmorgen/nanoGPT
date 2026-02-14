from __future__ import annotations

from typing import Any
from ml_playground.framework.core.protocols import (
    PersistenceStrategy,
    MLflowClient,
    MLflowRun,
)


def test_persistence_strategy_default_noop() -> None:
    # Line 90-91: _ = entries
    class DummyStrategy(PersistenceStrategy):
        def load(self):
            return {}

        def save(self, entries):
            super().save(entries)

    s = DummyStrategy()
    s.save({"a": 1})


def test_mlflow_client_default_noop() -> None:
    # Lines 197, 203
    class DummyClient(MLflowClient):
        def set_tracking_uri(self, uri: str) -> None:
            pass

        def get_experiment_by_name(self, name: str) -> Any:
            pass

        def set_experiment(self, name: str) -> Any:
            pass

        def create_experiment(self, name: str, **kwargs: object) -> str:
            return "1"

        def start_run(self, **kwargs: object) -> MLflowRun:
            class DummyRun:
                def __enter__(self) -> "DummyRun":
                    return self

                def __exit__(self, *args: object) -> None:
                    pass

                def __iter__(self) -> Any:
                    return iter([])

            return DummyRun()  # type: ignore

        def end_run(self):
            pass

        def log_params(self, params):
            pass

        def log_metrics(self, metrics, *, step=None):
            pass

        def log_artifact(self, local_path, *, artifact_path=None):
            super().log_artifact(local_path, artifact_path=artifact_path)

        def log_artifacts(self, local_dir, *, artifact_path=None):
            super().log_artifacts(local_dir, artifact_path=artifact_path)

        def set_tag(self, key, value):
            pass

    c = DummyClient()
    c.log_artifact("p", artifact_path="ap")
    c.log_artifacts("d", artifact_path="ap")
