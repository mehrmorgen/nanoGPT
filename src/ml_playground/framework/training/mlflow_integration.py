"""MLflow experiment tracking integration."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, cast

from ml_playground.framework.core.protocols import (
    MLflowClient,
    MLflowRun,
    OSModule,
    PlatformModule,
    SysModule,
)

try:
    import mlflow  # type: ignore[import-not-found]
except (ImportError, Exception):
    mlflow = None

from ml_playground.framework.configuration.models import (
    RuntimeConfig,
    MetadataConfig,
    TrainerConfig,
)
from ml_playground.framework.core.di_implementations import (
    StdOSModule,
    StdPlatformModule,
    StdSysModule,
)
from ml_playground.framework.core.logging_protocol import LoggerLike


class MLflowManager:
    """Manages MLflow lifecycle and logging."""

    def __init__(
        self,
        cfg: RuntimeConfig,
        metadata: MetadataConfig,
        logger: LoggerLike,
        mlflow_client: Optional[MLflowClient] = None,
        os_module: Optional[OSModule] = None,
        platform_module: Optional[PlatformModule] = None,
        sys_module: Optional[SysModule] = None,
    ):
        self.cfg = cfg
        self.metadata = metadata
        self.logger = logger
        self._mlflow = (
            mlflow_client if mlflow_client is not None else cast(MLflowClient, mlflow)
        )
        self._os = os_module if os_module is not None else StdOSModule()
        self._platform = (
            platform_module if platform_module is not None else StdPlatformModule()
        )
        self._sys = sys_module if sys_module is not None else StdSysModule()
        self._active_run: Optional[MLflowRun] = None

    def setup(self) -> None:
        """Initialize MLflow experiment and start run."""
        if not self.cfg.mlflow_enabled:
            return

        try:
            if self.cfg.mlflow_tracking_uri is not None:
                self._mlflow.set_tracking_uri(str(self.cfg.mlflow_tracking_uri))

            exp_name = self.cfg.mlflow_experiment_name or self.metadata.experiment

            artifact_root = self.metadata.config_path.parent / ".." / "out" / "mlruns"
            try:
                artifact_root.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            try:
                # Use a typed variable for the result to satisfy strict mode.
                existing = self._mlflow.get_experiment_by_name(exp_name)
            except Exception:
                existing = None

            if existing is None:
                try:
                    self._mlflow.create_experiment(
                        exp_name,
                        artifact_location=str(artifact_root),
                    )
                except Exception:
                    pass

            self._mlflow.set_experiment(exp_name)

            self._active_run = self._mlflow.start_run(
                run_name=self.cfg.mlflow_run_name,
                description=f"Training run for {self.metadata.experiment}",
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
            if self.metadata.config_path.exists():
                self._mlflow.log_artifact(
                    str(self.metadata.config_path), artifact_path="config"
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
