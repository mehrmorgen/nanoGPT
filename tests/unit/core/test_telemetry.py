from __future__ import annotations
from ml_playground.core.telemetry import NoOpTelemetry, ConsoleTelemetry


def test_noop_telemetry():
    tel = NoOpTelemetry()
    tel.log_metric("test", 1.0)
    with tel.time_block("test"):
        pass


def test_console_telemetry_logs(capsys):
    tel = ConsoleTelemetry()
    tel.log_metric("test_metric", 42.0, step=10)
    out, _ = capsys.readouterr()
    assert "[Metric] test_metric: 42.0 (step: 10)" in out


def test_console_telemetry_time_block(capsys):
    tel = ConsoleTelemetry()
    with tel.time_block("test_perf"):
        pass
    out, _ = capsys.readouterr()
    assert "[Perf] test_perf took" in out


def test_console_telemetry_with_logger():
    class MockLogger:
        def __init__(self):
            self.logs = []

        def info(self, msg):
            self.logs.append(msg)

    logger = MockLogger()
    tel = ConsoleTelemetry(logger=logger)
    tel.log_metric("m", 1.0)
    with tel.time_block("p"):
        pass
    assert any("[Metric] m: 1.0" in log for log in logger.logs)
    assert any("[Perf] p took" in log for log in logger.logs)
