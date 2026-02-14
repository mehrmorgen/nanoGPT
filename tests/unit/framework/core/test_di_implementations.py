from ml_playground.framework.core.di_implementations import (
    StdOSModule,
    StdPlatformModule,
    StdSysModule,
    NullMLflowClient,
    DefaultJsonParser,
    DefaultConfigSectionExtractor,
    DefaultTestResultExtractor,
    DefaultCoverageDataExtractor,
)


def test_std_os_module() -> None:
    os_mod = StdOSModule()
    assert isinstance(os_mod.getcwd(), str)
    assert isinstance(os_mod.getlogin(), str)


def test_std_platform_module() -> None:
    plat_mod = StdPlatformModule()
    assert isinstance(plat_mod.platform(), str)
    assert isinstance(plat_mod.processor(), str)


def test_std_sys_module() -> None:
    sys_mod = StdSysModule()
    assert isinstance(sys_mod.version, str)
    assert isinstance(sys_mod.argv, list)


def test_null_mlflow_client() -> None:
    client = NullMLflowClient()
    # Ensure no-op methods don't raise
    client.set_tracking_uri("http://localhost:5000")
    assert client.get_experiment_by_name("test") is None
    assert client.set_experiment("test") is None
    assert client.create_experiment("test") == ""
    run = client.start_run()
    assert run is not None
    with client.start_run() as entered:
        assert entered is not None
    client.end_run()
    client.log_params({"a": 1})
    client.log_metrics({"m": 0.1})
    client.log_artifact("file.txt")
    client.log_artifacts("dir")
    client.set_tag("k", "v")


def test_default_json_parser() -> None:
    parser = DefaultJsonParser()
    content = '{"key": "value"}'
    assert parser.parse_json(content) == {"key": "value"}
    assert parser.parse_gate_snapshot(content) == {"key": "value"}
    assert parser.parse_github_response(content) == {"key": "value"}


def test_default_config_section_extractor() -> None:
    extractor = DefaultConfigSectionExtractor()
    config = {"section": {"key": "value"}}
    assert extractor.extract_section(config, "section") == {"key": "value"}
    assert extractor.extract_section(config, "missing") == {}
    assert extractor.extract_section({"section": "not-a-dict"}, "section") == {}

    mapping = {"key": "value"}
    assert extractor.get_string(mapping, "key", "default") == "value"
    assert extractor.get_string(mapping, "missing", "default") == "default"
    assert extractor.get_string({"key": None}, "key", "default") == "default"
    assert extractor.get_string({"key": 123}, "key", "default") == "default"


def test_default_test_result_extractor() -> None:
    extractor = DefaultTestResultExtractor()
    results: dict[str, object] = {"overall": {"status": "passed"}}
    assert extractor.extract_overall(results) == {"status": "passed"}
    assert extractor.extract_overall({"missing": {}}) == {}
    assert extractor.extract_overall({"overall": "not-a-dict"}) == {}

    assert extractor.extract_status({"status": "failed"}) == "failed"
    assert extractor.extract_status({}) == "unknown"
    assert extractor.extract_status({"status": 123}) == "unknown"


def test_default_coverage_data_extractor() -> None:
    extractor = DefaultCoverageDataExtractor()
    data: dict[str, object] = {"totals": {"percent_covered": 95.5}}
    assert extractor.extract_totals(data) == {"percent_covered": 95.5}
    assert extractor.extract_totals({"missing": {}}) == {}

    assert extractor.get_coverage_percent({"percent_covered": 88.8}) == 88.8
    assert extractor.get_coverage_percent({}) == 0.0
    assert extractor.get_coverage_percent({"percent_covered": "not-a-num"}) == 0.0


def test_extract_totals_non_dict() -> None:
    """extract_totals returns {} when totals value is not a dict."""
    extractor = DefaultCoverageDataExtractor()
    assert extractor.extract_totals({"totals": "not-a-dict"}) == {}
