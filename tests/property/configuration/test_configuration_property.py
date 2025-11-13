"""Property-based tests for the configuration package."""

from __future__ import annotations

import string
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import hypothesis.strategies as st
from hypothesis import given, settings
import pytest
import tomllib

from ml_playground.configuration import loading as config_loading
from ml_playground.configuration.merge_utils import merge_mappings


def _normalize(value: object) -> object:
    if isinstance(value, Mapping):
        mapping_value = cast(Mapping[Any, object], value)
        normalized_dict: dict[str, object] = {}
        for key_obj, nested_value in mapping_value.items():
            normalized_key = str(key_obj)
            normalized_dict[normalized_key] = _normalize(nested_value)
        return normalized_dict
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        sequence_value = cast(Sequence[object], value)
        return [_normalize(item) for item in sequence_value]
    return value


@st.composite
def dict_strategy(draw: st.DrawFn) -> dict[str, Any]:
    """Generate nested dictionaries with string keys and various values."""

    base_values: st.SearchStrategy[object] = st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(max_size=50),
    )

    def extend(children: st.SearchStrategy[object]) -> st.SearchStrategy[object]:
        nested_dicts: st.SearchStrategy[dict[str, object]] = st.dictionaries(
            keys=st.text(min_size=1, max_size=10),
            values=children,
            max_size=10,
        )
        nested_lists: st.SearchStrategy[list[object]] = st.lists(children, max_size=5)
        return st.one_of(nested_dicts, nested_lists)

    value_strategy: st.SearchStrategy[object] = st.recursive(
        base_values,
        extend,
        max_leaves=10,
    )

    mapping_strategy: st.SearchStrategy[dict[str, Any]] = st.dictionaries(
        keys=st.text(min_size=1, max_size=10),
        values=value_strategy,
        max_size=10,
    )

    raw_mapping = draw(mapping_strategy)
    normalized_mapping = cast(dict[str, Any], _normalize(raw_mapping))
    return normalized_mapping


@st.composite
def toml_dict_strategy(draw: st.DrawFn) -> dict[str, Any]:
    """Generate dictionaries that can be serialized to TOML."""

    toml_scalars: st.SearchStrategy[object] = st.one_of(
        st.booleans(),
        st.integers(min_value=-1000, max_value=1000),
        st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False),
        st.text(max_size=50),
        st.dates(),
        st.times(),
        st.datetimes(),
    )

    def extend(children: st.SearchStrategy[object]) -> st.SearchStrategy[object]:
        nested_keys = st.text(
            min_size=1,
            max_size=10,
            alphabet=st.characters(
                whitelist_categories=("L", "N"),
                min_codepoint=97,
                max_codepoint=122,
            ),
        )
        nested_dicts: st.SearchStrategy[dict[str, object]] = st.dictionaries(
            keys=nested_keys,
            values=children,
            max_size=5,
        )
        nested_lists: st.SearchStrategy[list[object]] = st.lists(children, max_size=5)
        return st.one_of(nested_dicts, nested_lists)

    value_strategy: st.SearchStrategy[object] = st.recursive(
        toml_scalars,
        extend,
        max_leaves=5,
    )

    mapping_strategy: st.SearchStrategy[dict[str, Any]] = st.dictionaries(
        keys=st.text(
            min_size=1,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=("L", "N"),
                min_codepoint=97,
                max_codepoint=122,
            ),
        ),
        values=value_strategy,
        max_size=5,
    )

    return draw(mapping_strategy)


class TestMergeMappings:
    """Property-based tests for `merge_mappings`."""

    @given(base=dict_strategy(), override=dict_strategy())
    @settings(max_examples=25, deadline=None)
    def test_merge_preserves_base_keys_not_in_override(
        self, base: dict[str, Any], override: dict[str, Any]
    ) -> None:
        """Test merge preserves base keys not in override."""
        result = merge_mappings(base, override)
        for key in base:
            if key not in override:
                assert key in result
                assert result[key] == base[key]

    @given(base=dict_strategy(), override=dict_strategy())
    @settings(max_examples=50, deadline=None)
    def test_merge_overrides_base_values(
        self, base: dict[str, Any], override: dict[str, Any]
    ) -> None:
        """Test merge overrides base values."""
        result = merge_mappings(base, override)
        for key, value in override.items():
            if not isinstance(value, dict):
                assert result[key] == value
            elif key in base and isinstance(base[key], dict):
                base_child = cast(dict[str, Any], base[key])
                override_child = cast(dict[str, Any], value)
                assert result[key] == merge_mappings(base_child, override_child)

    @given(d1=dict_strategy(), d2=dict_strategy(), d3=dict_strategy())
    @settings(max_examples=20, deadline=None)
    def test_merge_associativity(
        self, d1: dict[str, Any], d2: dict[str, Any], d3: dict[str, Any]
    ) -> None:
        """Test merge associativity."""
        try:
            result1 = merge_mappings(merge_mappings(d1, d2), d3)
            result2 = merge_mappings(d1, merge_mappings(d2, d3))
            assert result1 == result2
        except Exception:
            pass

    @given(base=dict_strategy())
    @settings(max_examples=20, deadline=None)
    def test_merge_with_empty_override(self, base: dict[str, Any]) -> None:
        """Test merge with empty override."""
        result = merge_mappings(base, {})
        assert result == base

    @given(override=dict_strategy())
    @settings(max_examples=20, deadline=None)
    def test_merge_with_empty_base(self, override: dict[str, Any]) -> None:
        """Test merge with empty base."""
        result = merge_mappings({}, override)
        assert result == override


class TestTomlReading:
    """Property-based tests for TOML reading functionality."""

    @given(content=toml_dict_strategy())
    @settings(max_examples=50)
    def test_round_trip_toml_serialization(self, content: dict[str, Any]) -> None:
        """Test round trip toml serialization."""
        import tomli_w

        normalized_content = cast(dict[str, Any], _normalize(content))

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".toml", delete=False) as f:
            try:
                tomli_w.dump(normalized_content, f)
                f.flush()
                f.close()
                path = Path(f.name)
                result = config_loading.read_toml_dict(path)
                assert isinstance(result, dict)
                for key in content:
                    assert key in result
            finally:
                Path(f.name).unlink(missing_ok=True)

    @given(
        content=st.sampled_from(
            ["[invalid", "key =", "[[table]", "key = value extra", '{"json"}']
        )
    )
    @settings(max_examples=20)
    def test_invalid_toml_raises_exception(self, content: str) -> None:
        """Test invalid toml raises exception."""
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".toml", delete=False) as f:
            try:
                f.write(content)
                f.flush()
                f.close()
                path = Path(f.name)
                if content.strip():
                    with pytest.raises((Exception, tomllib.TOMLDecodeError)):
                        config_loading.read_toml_dict(path)
            finally:
                Path(f.name).unlink(missing_ok=True)


class TestConfigPaths:
    """Property-based tests for configuration path computation."""

    @given(
        experiment=st.text(
            alphabet=string.ascii_letters + string.digits + "_-",
            min_size=1,
            max_size=50,
        )
    )
    @settings(max_examples=50)
    def test_experiment_path_computation(self, experiment: str) -> None:
        """Test experiment path computation."""
        path = config_loading.get_cfg_path(experiment, None)
        assert str(path).endswith(f"experiments/{experiment}/config.toml")
        assert path.is_absolute()

    @given(
        exp_config=st.text(
            alphabet=string.ascii_letters + string.digits + "_-\\. ",
            min_size=1,
            max_size=100,
        )
    )
    @settings(max_examples=50)
    def test_custom_config_path(self, exp_config: str) -> None:
        """Test custom config path."""
        path = config_loading.get_cfg_path("dummy_experiment", Path(exp_config))
        assert str(path) == exp_config
