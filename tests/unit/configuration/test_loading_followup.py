from __future__ import annotations

from pathlib import Path

from ml_playground.configuration import loading as config_loading


def test_get_cfg_path_prefers_override(tmp_path: Path) -> None:
    override = tmp_path / "override.toml"
    assert config_loading.get_cfg_path("ignored", override) == override


def test_get_default_config_path_uses_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "project_root"
    project_root.mkdir()
    path = config_loading.get_default_config_path(project_root)
    assert path.name == "default_config.toml"


def test_list_experiments_with_config_missing_root(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    assert config_loading.list_experiments_with_config(experiments_root=missing) == []


def test_get_default_config_path_with_installed_project_root() -> None:
    """Explicit project_root equal to package parent should use package defaults.

    This simulates running from an installed package where the project root is the
    parent of the package directory.
    """
    package_root = Path(config_loading.__file__).resolve().parent.parent
    project_root = package_root.parent

    path = config_loading.get_default_config_path(project_root)
    assert path.name == "default_config.toml"
    assert (
        str(path)
        .replace("\\", "/")
        .endswith("src/ml_playground/experiments/default_config.toml")
    )


def test_load_sample_config_honors_default_config_path(tmp_path: Path) -> None:
    # default config providing the required sample defaults
    default_path = (
        tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )
    default_path.parent.mkdir(parents=True)
    default_path.write_text(
        """
[sample.runtime]
out_dir = "out/sample"
log_interval = 7

[sample.sample]
start = "\\n"
        """,
        encoding="utf-8",
    )

    # minimal cfg with top-level [sample] present but no content
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("[sample]\n", encoding="utf-8")

    cfg = config_loading.load_sample_config(cfg_path, default_config_path=default_path)
    # merged from defaults
    assert str(cfg.runtime.out_dir).endswith("out/sample")
    assert cfg.runtime.log_interval == 7
    assert cfg.sample.start == "\n"


def test_load_prepare_config_honors_default_config_path(tmp_path: Path) -> None:
    default_path = (
        tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )
    default_path.parent.mkdir(parents=True)
    default_path.write_text(
        """
[prepare]
tokenizer_type = "char"
        """,
        encoding="utf-8",
    )

    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("[prepare]\n", encoding="utf-8")

    cfg = config_loading.load_prepare_config(cfg_path, default_config_path=default_path)
    assert cfg.tokenizer_type == "char"
    assert "provenance" in cfg.extras


def test_default_config_path_from_root_variants(tmp_path: Path) -> None:
    root_ml = tmp_path / "ml_playground"
    root_ml.mkdir()
    assert config_loading._default_config_path_from_root(root_ml) == (
        root_ml / "experiments" / "default_config.toml"
    )

    root_src = tmp_path / "src"
    root_src.mkdir()
    assert config_loading._default_config_path_from_root(root_src) == (
        root_src / "ml_playground" / "experiments" / "default_config.toml"
    )

    root_other = tmp_path / "project"
    root_other.mkdir()
    assert config_loading._default_config_path_from_root(root_other) == (
        root_other / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )


def test_load_and_merge_configs_includes_ldres_override(tmp_path: Path) -> None:
    project_home = tmp_path / "workspace"
    project_home.mkdir()
    exp_name = "unit_exp"

    defaults_path = (
        project_home / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )
    defaults_path.parent.mkdir(parents=True)
    defaults_path.write_text('[shared]\nexperiment = "defaults"\n', encoding="utf-8")

    cfg_path = project_home / "config.toml"
    cfg_path.write_text('[shared]\nexperiment = "raw"\n', encoding="utf-8")

    ldres_path = (
        project_home
        / ".ldres"
        / "etc"
        / "ml_playground"
        / "experiments"
        / exp_name
        / "config.toml"
    )
    ldres_path.parent.mkdir(parents=True)
    ldres_path.write_text('[shared]\nexperiment = "ldres"\n', encoding="utf-8")

    merged = config_loading._load_and_merge_configs(cfg_path, project_home, exp_name)
    assert merged["shared"]["experiment"] == "ldres"
