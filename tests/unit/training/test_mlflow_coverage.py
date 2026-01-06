from ml_playground.configuration.models import RuntimeConfig, SharedConfig
from ml_playground.training.mlflow_integration import MLflowManager


class FakeLogger:
    def __init__(self):
        self.debugs = []
        self.warnings = []

    def debug(self, msg, *a, **k):
        self.debugs.append(msg)

    def warning(self, msg, *a, **k):
        self.warnings.append(msg)


class FakeMLflowClient:
    def __init__(self):
        self.tracking_uri = None
        self.experiment_name = None
        self.tags = {}
        self.params = []
        self.artifacts = []
        self.run_ended = False
        self.metrics = []

    def set_tracking_uri(self, uri):
        self.tracking_uri = uri

    def set_experiment(self, name):
        self.experiment_name = name

    def start_run(self, **kwargs):
        return object()

    def end_run(self):
        self.run_ended = True

    def log_params(self, params):
        self.params.append(params)

    def log_metrics(self, metrics, step=None):
        self.metrics.append((metrics, step))

    def log_artifact(self, local, artifact_path=None):
        self.artifacts.append((local, artifact_path))

    def log_artifacts(self, local, artifact_path=None):
        self.artifacts.append((local, artifact_path))

    def set_tag(self, k, v):
        self.tags[k] = v

    def create_experiment(self, name, artifact_location=None):
        return "exp_id"


def test_mlflow_manager_setup_tracking_uri_branch(tmp_path):
    """Branch coverage for MLflowManager.setup tracking_uri."""
    cfg = RuntimeConfig(out_dir=tmp_path, mlflow_tracking_uri=tmp_path / "mlflow")
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()
    manager = MLflowManager(cfg, shared, FakeLogger(), mlflow_client=client)
    manager.setup()
    assert client.tracking_uri == str(tmp_path / "mlflow")


def test_mlflow_manager_setup_create_experiment_fails(tmp_path):
    """Branch coverage for MLflowManager.setup create_experiment exception."""
    cfg = RuntimeConfig(out_dir=tmp_path, mlflow_artifact_root="mlflow_artifacts")
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()

    def broken_create(name, artifact_location=None):
        raise Exception("already exists")

    client.create_experiment = broken_create
    manager = MLflowManager(cfg, shared, FakeLogger(), mlflow_client=client)
    # Should not raise
    manager.setup()


def test_mlflow_manager_log_reproducibility_user_fails(tmp_path):
    """Branch coverage for _log_reproducibility_info user fetch failures."""
    cfg = RuntimeConfig(out_dir=tmp_path)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()

    class BrokenOS:
        def getcwd(self):
            return "cwd"

        def getlogin(self):
            raise OSError("no user")

    manager = MLflowManager(
        cfg, shared, FakeLogger(), mlflow_client=client, os_module=BrokenOS()
    )
    manager._log_reproducibility_info()
    assert client.tags["mlflow.user"] == "unknown"


def test_mlflow_manager_log_artifact_not_active(tmp_path):
    """Branch coverage for log_artifact when not active."""
    cfg = RuntimeConfig(out_dir=tmp_path)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    manager = MLflowManager(cfg, shared, FakeLogger())
    manager.log_artifact(tmp_path)  # Should be no-op


def test_mlflow_manager_log_artifact_dir_vs_file(tmp_path):
    """Branch coverage for log_artifact dir vs file."""
    cfg = RuntimeConfig(out_dir=tmp_path)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()
    manager = MLflowManager(cfg, shared, FakeLogger(), mlflow_client=client)
    manager._active_run = object()

    # Dir
    d = tmp_path / "dir"
    d.mkdir()
    manager.log_artifact(d)

    # File
    f = tmp_path / "file.txt"
    f.write_text("hi")
    manager.log_artifact(f)
    assert len(client.artifacts) == 2


def test_mlflow_manager_log_metrics_exception(tmp_path):
    """Branch coverage for log_metrics exception path."""
    cfg = RuntimeConfig(out_dir=tmp_path)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()

    def broken_log_metrics(m, step=None):
        raise Exception("mlflow down")

    client.log_metrics = broken_log_metrics
    logger = FakeLogger()
    manager = MLflowManager(cfg, shared, logger, mlflow_client=client)
    manager._active_run = object()
    manager.log_metrics({"loss": 0.1}, 1)
    assert any("metric logging failed" in msg for msg in logger.debugs)


def test_mlflow_manager_log_artifact_exception(tmp_path):
    """Branch coverage for log_artifact exception path."""
    cfg = RuntimeConfig(out_dir=tmp_path)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()

    def broken_log_artifact(local_path, artifact_path=None):
        raise Exception("upload failed")

    client.log_artifact = broken_log_artifact
    logger = FakeLogger()
    manager = MLflowManager(cfg, shared, logger, mlflow_client=client)
    manager._active_run = object()
    manager.log_artifact(tmp_path / "file.txt")
    assert any("artifact logging failed" in msg for msg in logger.warnings)


def test_mlflow_manager_setup_exception(tmp_path):
    """Branch coverage for setup top-level exception path."""
    cfg = RuntimeConfig(out_dir=tmp_path, mlflow_enabled=True)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    client = FakeMLflowClient()
    client.start_run = lambda **k: exec('raise(Exception("critical failure"))')
    logger = FakeLogger()
    manager = MLflowManager(cfg, shared, logger, mlflow_client=client)
    manager.setup()
    assert any("MLflow setup failed" in msg for msg in logger.warnings)
    assert manager._active_run is None
