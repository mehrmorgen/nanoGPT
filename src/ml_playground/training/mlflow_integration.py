"""MLflow experiment tracking integration."""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path
from typing import Any, Dict, Optional, TypeAlias, cast

import mlflow
from ml_playground.configuration.models import (
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.core.logging_protocol import LoggerLike


MLflowClient: TypeAlias = Any
OSModule: TypeAlias = Any
PlatformModule: TypeAlias = Any
SysModule: TypeAlias = Any


class MLflowManager:
    """Manages MLflow lifecycle and logging."""

    def __init__(
        self,
        cfg: RuntimeConfig,
        shared: SharedConfig,
        logger: LoggerLike,
        mlflow_client: Optional[MLflowClient] = None,
        os_module: Optional[OSModule] = None,
        platform_module: Optional[PlatformModule] = None,
        sys_module: Optional[SysModule] = None,
    ):
        self.cfg = cfg
        self.shared = shared
        self.logger = logger
        self._mlflow = (
            mlflow_client if mlflow_client is not None else cast(MLflowClient, mlflow)
        )
        self._os = os_module if os_module is not None else cast(OSModule, os)
        self._platform = (
            platform_module
            if platform_module is not None
            else cast(PlatformModule, platform)
        )
        self._sys = sys_module if sys_module is not None else cast(SysModule, sys)
        self._active_run: Optional[Any] = None

    def setup(self) -> None:
        """Initialize MLflow experiment and start run."""
        if not self.cfg.mlflow_enabled:
            return

        try:
            self._mlflow.set_tracking_uri(str(self.cfg.mlflow_tracking_uri))

            exp_name = self.cfg.mlflow_experiment_name or self.shared.experiment
            self._mlflow.set_experiment(exp_name)

            try:
                artifact_root = self.cfg.mlflow_artifact_root
                if artifact_root is not None:
                    artifact_location = str(Path(artifact_root).resolve())
                    self._mlflow.create_experiment(
                        self.shared.experiment, artifact_location=artifact_location
                    )
            except Exception:
                pass

            self._active_run = self._mlflow.start_run(
                run_name=self.cfg.mlflow_run_name,
                description=f"Training run for {self.shared.experiment}",
            )

            # Log system info for reproducibility
            self._log_reproducibility_info()

            if self.cfg.mlflow_log_system_metrics:
                try:
                    self._mlflow.set_tag(
                        "mlflow.note.content", "System metrics enabled"
                    )
                except Exception as exc:
                    self.logger.debug(f"Failed to set MLflow system metrics tag: {exc}")

        except Exception as exc:
            self.logger.warning(f"MLflow setup failed: {exc}")
            self._active_run = None

    def _log_reproducibility_info(self) -> None:
        """Log environment and seed information."""
        params = {
            "reproducibility.seed": self.cfg.seed,
            "env.python_version": self._sys.version.split()[0],
            "env.platform": self._platform.platform(),
            "env.processor": self._platform.processor(),
            "env.cwd": self._os.getcwd(),
        }
        self._mlflow.log_params(params)
        self._mlflow.set_tag("mlflow.source.name", self._sys.argv[0])
        try:
            user = self._os.getlogin()
        except (AttributeError, OSError, RuntimeError):
            user = "unknown"
        self._mlflow.set_tag("mlflow.user", user)

    def log_config(self, trainer_cfg: TrainerConfig) -> None:
        """Log full configuration as parameters and artifact."""
        if not self._active_run:
            return

        try:
            # Flatten config for parameters (selective)
            params = {
                "model.n_layer": trainer_cfg.model.n_layer,
                "model.n_head": trainer_cfg.model.n_head,
                "model.n_embd": trainer_cfg.model.n_embd,
                "model.block_size": trainer_cfg.model.block_size,
                "optim.learning_rate": trainer_cfg.optim.learning_rate,
                "runtime.max_iters": self.cfg.max_iters,
                "data.batch_size": trainer_cfg.data.batch_size,
                "data.grad_accum_steps": trainer_cfg.data.grad_accum_steps,
            }
            self._mlflow.log_params(params)

            # Log raw config file as artifact for full versioning
            if self.shared.config_path.exists():
                self._mlflow.log_artifact(
                    str(self.shared.config_path), artifact_path="config"
                )
        except Exception as exc:
            self.logger.warning(f"MLflow config logging failed: {exc}")

    def log_metrics(self, metrics: Dict[str, float], step: int) -> None:
        """Log metrics to MLflow."""
        if not self._active_run:
            return

        try:
            self._mlflow.log_metrics(metrics, step=step)
        except Exception as exc:
            self.logger.debug(f"MLflow metric logging failed: {exc}")

    def log_artifact(
        self, local_path: Path, artifact_path: Optional[str] = None
    ) -> None:
        """Log a file or directory as an artifact."""
        if not self._active_run:
            return

        try:
            if local_path.is_dir():
                self._mlflow.log_artifacts(str(local_path), artifact_path=artifact_path)
            else:
                self._mlflow.log_artifact(str(local_path), artifact_path=artifact_path)
        except Exception as exc:
            self.logger.warning(
                f"MLflow artifact logging failed for {local_path}: {exc}"
            )

    def finish(self) -> None:
        """Close the MLflow run."""
        if self._active_run:
            self._mlflow.end_run()
            self._active_run = None
