"""Unit tests for extras_registry.py branch coverage.

Tests uncovered branches in load_extras_models for exception handling
and edge cases.
"""

from __future__ import annotations

import sys
from types import ModuleType

from pydantic import BaseModel

from ml_playground.framework.experiment_registry.extras_registry import (
    EXTRAS_MODELS,
    LOADED_EXPERIMENTS,
    get_extras_model,
    load_extras_models,
    register_extras_model,
)


class FakeModule(ModuleType):
    """Fake module for testing imports."""

    def __init__(self, name: str) -> None:
        super().__init__(name)


class FakeImportSystem:
    """Fake import system for testing."""

    def __init__(self, modules: dict[str, ModuleType]) -> None:
        self.modules = modules
        self.import_calls: list[str] = []

    def import_mod(self, name: str) -> ModuleType:
        self.import_calls.append(name)
        if name in self.modules:
            return self.modules[name]
        raise ImportError(f"No module named '{name}'")


def test_load_extras_models_already_loaded() -> None:
    """Test early return when experiment already in LOADED_EXPERIMENTS."""
    # Setup
    LOADED_EXPERIMENTS.add("test_already_loaded")

    try:
        # Call function - should return early without raising
        load_extras_models("test_already_loaded")
        # If we get here, the early return worked
    finally:
        # Cleanup
        LOADED_EXPERIMENTS.discard("test_already_loaded")


def test_load_extras_models_import_error() -> None:
    """Test handling of ImportError for non-existent experiment."""
    experiment_name = "nonexistent_experiment_xyz"

    # Ensure not already loaded
    LOADED_EXPERIMENTS.discard(experiment_name)

    # This should not raise - ImportError is caught internally
    load_extras_models(experiment_name)

    # Verify experiment was not added to LOADED
    assert experiment_name not in LOADED_EXPERIMENTS


def test_load_extras_models_no_models_after_import() -> None:
    """Test when no models registered after import (line 71-73)."""
    experiment_name = "test_no_models_after_import"
    module_path = f"ml_playground.experiments.{experiment_name}.extras"

    # Create a fake module (no models registered)
    fake_module = FakeModule(module_path)

    fake_import = FakeImportSystem({module_path: fake_module})

    # Ensure not already loaded
    LOADED_EXPERIMENTS.discard(experiment_name)
    if experiment_name in EXTRAS_MODELS:
        del EXTRAS_MODELS[experiment_name]

    try:
        # Call function with fake import
        load_extras_models(experiment_name, import_mod=fake_import.import_mod)

        # Verify experiment was not added to LOADED (no models registered)
        assert experiment_name not in LOADED_EXPERIMENTS
        # Verify import was called
        assert fake_import.import_calls == [module_path]
    finally:
        LOADED_EXPERIMENTS.discard(experiment_name)
        if experiment_name in EXTRAS_MODELS:
            del EXTRAS_MODELS[experiment_name]


def test_load_extras_models_file_based_import_success() -> None:
    """Test successful file-based import fallback (lines 78-91).

    Creates a temporary experiment under the real experiments directory.
    The extras.py script accesses ``EXTRAS_MODELS`` via ``sys.modules``
    to avoid issues with pytest's import rewriting hooks.
    """
    import shutil

    import ml_playground.framework.experiment_registry.extras_registry as extras_reg

    extras_root = (
        __import__("pathlib").Path(extras_reg.__file__).resolve().parents[2]
        / "experiments"
    )
    experiment_name = "_test_file_import_fb"
    module_path = f"ml_playground.experiments.{experiment_name}.extras"
    experiment_dir = extras_root / experiment_name

    # Use the module's live references to avoid stale bindings after reloads
    extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
    extras_reg.EXTRAS_MODELS.pop(experiment_name, None)

    try:
        experiment_dir.mkdir(parents=True, exist_ok=True)
        extras_file = experiment_dir / "extras.py"
        # Pre-register the model so that the EXTRAS_MODELS check on line 90
        # succeeds after exec_module runs the no-op extras.py.
        extras_file.write_text("pass\n")
        extras_reg.register_extras_model(
            experiment_name,
            "test_section",
            type("M", (), {}),  # type: ignore[arg-type]
        )
        assert extras_reg.EXTRAS_MODELS.get(experiment_name), (
            f"Pre-registration failed; keys={list(extras_reg.EXTRAS_MODELS)}"
        )

        def always_fail_import(name: str) -> ModuleType:
            raise ImportError(f"No module named '{name}'")

        extras_reg.load_extras_models(experiment_name, import_mod=always_fail_import)

        assert experiment_name in extras_reg.LOADED_EXPERIMENTS, (
            f"Expected {experiment_name} in LOADED={extras_reg.LOADED_EXPERIMENTS}; "
            f"EXTRAS_MODELS.get={extras_reg.EXTRAS_MODELS.get(experiment_name)}"
        )
        assert extras_reg.get_extras_model(experiment_name, "test_section") is not None

    finally:
        extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
        extras_reg.EXTRAS_MODELS.pop(experiment_name, None)
        sys.modules.pop(module_path, None)
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)


def test_load_extras_models_registers_on_first_import() -> None:
    """When the imported module registers models immediately, load marks it loaded (lines 63-65)."""
    import ml_playground.framework.experiment_registry.extras_registry as extras_reg

    experiment_name = "_test_immediate_register"
    module_path = f"ml_playground.experiments.{experiment_name}.extras"

    extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
    extras_reg.EXTRAS_MODELS.pop(experiment_name, None)

    class _RegisteringModule(ModuleType):
        pass

    fake_mod = _RegisteringModule(module_path)

    def _import_and_register(name: str) -> ModuleType:
        extras_reg.register_extras_model(
            experiment_name,
            "s",
            type("M", (BaseModel,), {"__annotations__": {"x": int}}),
        )
        return fake_mod

    try:
        extras_reg.load_extras_models(experiment_name, import_mod=_import_and_register)
        assert experiment_name in extras_reg.LOADED_EXPERIMENTS
    finally:
        extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
        extras_reg.EXTRAS_MODELS.pop(experiment_name, None)


def test_load_extras_models_reload_path_no_models() -> None:
    """Import returns ModuleType without registering; reload fails; no models (lines 66-73)."""
    experiment_name = "_test_reload_no_models"
    module_path = f"ml_playground.experiments.{experiment_name}.extras"

    LOADED_EXPERIMENTS.discard(experiment_name)
    EXTRAS_MODELS.pop(experiment_name, None)

    fake_mod = FakeModule(module_path)
    sys.modules[module_path] = fake_mod

    def _import_no_register(name: str) -> ModuleType:
        return fake_mod

    try:
        load_extras_models(experiment_name, import_mod=_import_no_register)
        assert experiment_name not in LOADED_EXPERIMENTS
    finally:
        LOADED_EXPERIMENTS.discard(experiment_name)
        EXTRAS_MODELS.pop(experiment_name, None)
        sys.modules.pop(module_path, None)


def test_load_extras_models_import_returns_non_module() -> None:
    """When import returns non-ModuleType, reload is skipped (branch 66→71)."""
    experiment_name = "_test_non_module_return"

    LOADED_EXPERIMENTS.discard(experiment_name)
    EXTRAS_MODELS.pop(experiment_name, None)

    def _import_returns_namespace(name: str) -> object:
        return {"not": "a module"}

    try:
        load_extras_models(experiment_name, import_mod=_import_returns_namespace)  # type: ignore[arg-type]
        assert experiment_name not in LOADED_EXPERIMENTS
    finally:
        LOADED_EXPERIMENTS.discard(experiment_name)
        EXTRAS_MODELS.pop(experiment_name, None)


def test_load_extras_models_reload_triggers_registration() -> None:
    """Reload of a real module triggers registration, covering lines 71-73."""
    import shutil
    import importlib

    import ml_playground.framework.experiment_registry.extras_registry as extras_reg

    extras_root = (
        __import__("pathlib").Path(extras_reg.__file__).resolve().parents[2]
        / "experiments"
    )
    experiment_name = "_test_reload_triggers"
    module_path = f"ml_playground.experiments.{experiment_name}.extras"
    experiment_dir = extras_root / experiment_name

    extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
    extras_reg.EXTRAS_MODELS.pop(experiment_name, None)
    sys.modules.pop(module_path, None)

    try:
        experiment_dir.mkdir(parents=True, exist_ok=True)
        (experiment_dir / "__init__.py").write_text("")
        # Write extras.py that registers a model on import
        extras_file = experiment_dir / "extras.py"
        extras_file.write_text(
            "import sys\n"
            "from types import ModuleType\n"
            "from pydantic import BaseModel\n"
            "_store_key = 'ml_playground._extras_registry_store'\n"
            "_store = sys.modules.get(_store_key)\n"
            "if isinstance(_store, ModuleType):\n"
            "    _models = getattr(_store, 'models', {})\n"
            f"    _models.setdefault('{experiment_name}', {{}})['s'] = type('M', (BaseModel,), {{'__annotations__': {{'x': int}}}})\n"
        )

        # First import: returns the real module AND registers models.
        # However load_extras_models checks EXTRAS_MODELS BEFORE reload.
        # To hit 71-73 we need: first import succeeds, EXTRAS_MODELS empty (line 63 False),
        # reload succeeds and registers, then line 71 is True.
        #
        # Strategy: import first WITHOUT registration, then set up extras.py for reload.

        # Step 1: Write a no-op extras.py and import it
        extras_file.write_text("# no-op first import\n")
        first_mod = importlib.import_module(module_path)
        assert experiment_name not in extras_reg.EXTRAS_MODELS

        # Step 2: Rewrite extras.py to register on import
        extras_file.write_text(
            "import sys\n"
            "from types import ModuleType\n"
            "from pydantic import BaseModel\n"
            "_store_key = 'ml_playground._extras_registry_store'\n"
            "_store = sys.modules.get(_store_key)\n"
            "if isinstance(_store, ModuleType):\n"
            "    _models = getattr(_store, 'models', {})\n"
            f"    _models.setdefault('{experiment_name}', {{}})['s'] = type('M', (BaseModel,), {{'__annotations__': {{'x': int}}}})\n"
        )

        # Step 3: Call load_extras_models. The import returns the cached module
        # (no models yet → line 63 False), isinstance True → reload → registration
        # → line 71 True → lines 72-73 executed.
        def _return_cached(name: str) -> ModuleType:
            return first_mod

        extras_reg.load_extras_models(experiment_name, import_mod=_return_cached)

        assert experiment_name in extras_reg.LOADED_EXPERIMENTS

    finally:
        extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
        extras_reg.EXTRAS_MODELS.pop(experiment_name, None)
        sys.modules.pop(module_path, None)
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)


def test_load_extras_models_file_fallback_exec_error() -> None:
    """File-based import fallback handles exec_module errors (lines 88-89)."""
    import shutil

    import ml_playground.framework.experiment_registry.extras_registry as extras_reg

    extras_root = (
        __import__("pathlib").Path(extras_reg.__file__).resolve().parents[2]
        / "experiments"
    )
    experiment_name = "_test_exec_error_fb"
    experiment_dir = extras_root / experiment_name

    extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
    extras_reg.EXTRAS_MODELS.pop(experiment_name, None)

    try:
        experiment_dir.mkdir(parents=True, exist_ok=True)
        extras_file = experiment_dir / "extras.py"
        extras_file.write_text("raise RuntimeError('boom')\n")

        def always_fail_import(name: str) -> ModuleType:
            raise ImportError(f"No module named '{name}'")

        extras_reg.load_extras_models(experiment_name, import_mod=always_fail_import)

        assert experiment_name not in extras_reg.LOADED_EXPERIMENTS
    finally:
        extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
        extras_reg.EXTRAS_MODELS.pop(experiment_name, None)
        sys.modules.pop(f"ml_playground.experiments.{experiment_name}.extras", None)
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)


def test_load_extras_models_file_fallback_no_models() -> None:
    """File-based import succeeds but no models registered (line 90→exit)."""
    import shutil

    import ml_playground.framework.experiment_registry.extras_registry as extras_reg

    extras_root = (
        __import__("pathlib").Path(extras_reg.__file__).resolve().parents[2]
        / "experiments"
    )
    experiment_name = "_test_no_models_fb"
    experiment_dir = extras_root / experiment_name

    extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
    extras_reg.EXTRAS_MODELS.pop(experiment_name, None)

    try:
        experiment_dir.mkdir(parents=True, exist_ok=True)
        extras_file = experiment_dir / "extras.py"
        extras_file.write_text("# no models registered\n")

        def always_fail_import(name: str) -> ModuleType:
            raise ImportError(f"No module named '{name}'")

        extras_reg.load_extras_models(experiment_name, import_mod=always_fail_import)

        assert experiment_name not in extras_reg.LOADED_EXPERIMENTS
    finally:
        extras_reg.LOADED_EXPERIMENTS.discard(experiment_name)
        extras_reg.EXTRAS_MODELS.pop(experiment_name, None)
        sys.modules.pop(f"ml_playground.experiments.{experiment_name}.extras", None)
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)


def test_register_and_get_extras_model() -> None:
    """Test register_extras_model and get_extras_model functions."""

    class TestModel(BaseModel):
        value: int

    # Register a model
    register_extras_model("test_experiment", "test_section", TestModel)

    # Verify it can be retrieved
    retrieved = get_extras_model("test_experiment", "test_section")
    assert retrieved == TestModel

    # Verify missing experiment returns None
    missing_exp = get_extras_model("nonexistent", "test_section")
    assert missing_exp is None

    # Verify missing section returns None
    missing_section = get_extras_model("test_experiment", "nonexistent")
    assert missing_section is None

    # Cleanup
    if "test_experiment" in EXTRAS_MODELS:
        del EXTRAS_MODELS["test_experiment"]
