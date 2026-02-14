from __future__ import annotations

from pathlib import Path

from ml_playground.framework.configuration import loading as config_loading


def test_get_default_config_path_with_installed_project_root() -> None:
    """Test project_root pointing to package parent uses package defaults."""
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
    """Test load sample config honors default config path."""
    # default config providing the required sample defaults
    default_path = (
        tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )
    default_path.parent.mkdir(parents=True)
    default_path.write_text(
        """
[sampling.runtime]
out_dir = "out/sample"
log_interval = 7

[sampling.sample]
start = "\\n"
        """,
        encoding="utf-8",
    )

    # minimal cfg with top-level [sampling] present but no content
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("[sampling]\n", encoding="utf-8")

    cfg = config_loading.load_sample_config(cfg_path, default_config_path=default_path)
    # merged from defaults
    assert str(cfg.runtime.out_dir).endswith("out/sample")
    assert cfg.runtime.log_interval == 7
    assert cfg.sample.start == "\n"


def test_load_prepare_config_honors_default_config_path(tmp_path: Path) -> None:
    """Test load prepare config honors default config path."""
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
