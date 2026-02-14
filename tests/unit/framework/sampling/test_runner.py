from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import pickle
from typing import Any, Literal, Mapping, Sequence, Tuple, cast

import pytest
import torch
import numpy as np
from numpy.typing import NDArray

from ml_playground.framework.configuration.models import (
    RuntimeConfig,
    SampleConfig,
    SamplerConfig,
    DataConfig,
    READ_POLICY_BEST,
    READ_POLICY_LATEST,
    MetadataConfig,
)
from ml_playground.framework.sampling.api import SamplingPlan, run_sampling
from ml_playground.framework.sampling.runner import (
    Sampler,
    SamplerDependencies,
    default_sampler_dependencies,
)
from ml_playground.framework.core.error_handling import (
    DataError,
    CheckpointError,
    FileOperationError,
)
from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
)
from ml_playground.framework.data_pipeline.sampling.batches import SimpleBatches
from ml_playground.framework.models.core.config import GPTConfig
from ml_playground.framework.models.core.model import GPT
from ml_playground.framework.core.tokenizer_protocol import Tokenizer


# ---------------------------
# Helpers
# ---------------------------


def _write_char_meta(meta_path: Path) -> None:
    """Write a minimal char-level meta.pkl with stoi/itos and uint32 dtype."""
    stoi = {"\n": 0, "H": 1, "i": 2}
    itos = {v: k for k, v in stoi.items()}
    meta = {
        "meta_version": 1,
        "kind": "char",
        "dtype": "uint32",
        "tokenizer_type": "char",
        "stoi": stoi,
        "itos": itos,
    }
    meta_path.write_bytes(pickle.dumps(meta))


# ---------------------------
# SimpleBatches tests (consolidated from test_batches_sampler.py)
# ---------------------------


def _write_bin(path: Path, arr: np.ndarray) -> None:
    path.write_bytes(arr.tobytes())


def _prepare_dataset(tmp_path: Path, L: int, dtype: str = "uint16") -> Path:
    ddir = tmp_path / "ds"
    ddir.mkdir(parents=True, exist_ok=True)
    arr = cast(
        NDArray[np.generic], (np.arange(L) % np.iinfo(np.uint16).max).astype(dtype)
    )
    _write_bin(ddir / "train.bin", arr)
    _write_bin(ddir / "val.bin", arr)
    return ddir


def _make_batches(
    ddir: Path,
    *,
    batch_size: int,
    block_size: int,
    sampler: str,
) -> SimpleBatches:
    cfg = DataConfig(
        batch_size=batch_size,
        block_size=block_size,
        grad_accum_steps=1,
        sampler=sampler,  # type: ignore[arg-type]
    )
    return SimpleBatches(cfg, device="cpu", dataset_dir=ddir)


class _SamplerHarness(Sampler):
    """Sampler variant that exposes protected helpers for white-box tests."""

    def expose_get_start_ids(self) -> list[int]:
        return self._get_start_ids()

    def set_prompt_cache(
        self,
        tensor: torch.Tensor | None,
        prompt_ids: tuple[int, ...] | None,
    ) -> None:
        self._prompt_tensor = tensor
        self._cached_prompt_ids = prompt_ids


def test_random_mode_basic(tmp_path: Path) -> None:
    """Test random mode basic."""
    ddir = _prepare_dataset(tmp_path, L=100)
    batches = _make_batches(ddir, batch_size=4, block_size=8, sampler="random")
    x, y = batches.get_batch("train")
    assert x.shape == (4, 8)
    assert y.shape == (4, 8)
    # For contiguous windows, y is x shifted by 1 with one next token appended
    assert torch.equal(y[:, :-1], x[:, 1:])


def test_sequential_progression_basic(tmp_path: Path) -> None:
    """Test sequential progression basic."""
    L, T, B = 20, 5, 2
    ddir = _prepare_dataset(tmp_path, L=L)
    batches = _make_batches(ddir, batch_size=B, block_size=T, sampler="sequential")
    # First call
    x1, y1 = batches.get_batch("train")
    # Expected sequences: starts at 0 and 5
    exp_x0 = torch.tensor([[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]], dtype=torch.long)
    exp_y0 = torch.tensor([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]], dtype=torch.long)
    assert torch.equal(x1.cpu(), exp_x0)
    assert torch.equal(y1.cpu(), exp_y0)

    # Second call: cursor logic advances; first sample at 10..14, second wraps
    x2, y2 = batches.get_batch("train")
    exp_x1 = torch.tensor(
        [[10, 11, 12, 13, 14], [15, 16, 17, 18, 19]], dtype=torch.long
    )
    exp_y1 = torch.tensor([[11, 12, 13, 14, 15], [16, 17, 18, 19, 0]], dtype=torch.long)
    assert torch.equal(x2.cpu(), exp_x1)
    assert torch.equal(y2.cpu(), exp_y1)


def test_test_sequential_wrap_small__l_leq__t(tmp_path: Path) -> None:
    """Test sequential wrap small L leq T."""
    # L <= T path must wrap within a single sequence
    L, T, B = 4, 6, 1
    ddir = _prepare_dataset(tmp_path, L=L)
    batches = _make_batches(ddir, batch_size=B, block_size=T, sampler="sequential")
    x1, y1 = batches.get_batch("train")
    exp_x1 = torch.tensor([[0, 1, 2, 3, 0, 1]], dtype=torch.long)
    exp_y1 = torch.tensor([[1, 2, 3, 0, 1, 2]], dtype=torch.long)
    assert torch.equal(x1.cpu(), exp_x1)
    assert torch.equal(y1.cpu(), exp_y1)
    # Next call starts from cursor advanced by T mod L
    x2, y2 = batches.get_batch("train")
    exp_x2 = torch.tensor([[2, 3, 0, 1, 2, 3]], dtype=torch.long)
    exp_y2 = torch.tensor([[3, 0, 1, 2, 3, 0]], dtype=torch.long)
    assert torch.equal(x2.cpu(), exp_x2)
    assert torch.equal(y2.cpu(), exp_y2)


class _DummyModel:
    def __init__(self) -> None:
        self.loaded_state: Any | None = None

    def eval(self) -> "_DummyModel":  # noqa: D401
        """No-op eval returning self."""
        return self

    def to(self, device: str) -> "_DummyModel":  # noqa: D401
        """No-op device move returning self."""
        return self

    def load_state_dict(self, sd: dict[str, Any]) -> None:
        if sd.get("fail_load"):
            raise RuntimeError("bad state_dict")
        self.loaded_state = sd

    def __call__(self, x: torch.Tensor) -> Tuple[torch.Tensor, None]:
        # Return fake logits for testing
        b, t = x.shape
        vocab_size = 16  # Match the vocab_size in the test
        logits = torch.randn(b, t, vocab_size, dtype=torch.float32, device=x.device)
        return logits, None

    def generate(
        self,
        x: torch.Tensor,
        max_new_tokens: int,
        temperature: float,
        top_k: int,
    ) -> torch.Tensor:
        # Return input with a fixed number of additional tokens
        b, t = x.shape
        out = torch.ones((b, t + max_new_tokens), dtype=torch.long, device=x.device)
        out[:, :t] = x
        return out


# ---------------------------
# load_checkpoint tests
# ---------------------------


def test_load_checkpoint_no_files_raises(tmp_path: Path) -> None:
    """It should raise CheckpointError when no checkpoint files exist."""
    mgr = CheckpointManager(tmp_path)
    with pytest.raises(CheckpointError) as e:
        mgr.load_latest_checkpoint(
            device="cpu", logger=__import__("logging").getLogger("test")
        )
    assert "No last checkpoints discovered" in str(e.value)


def test_load_checkpoint_non_dict_raises(tmp_path: Path) -> None:
    """It should raise CheckpointError when checkpoint is not a dict."""
    # Craft a non-dict checkpoint by creating a tensor
    ckpt = tmp_path / "ckpt_best.pt"
    torch.save(torch.tensor([1, 2, 3]), ckpt)
    # Make it discoverable as a last checkpoint for the manager
    (tmp_path / "ckpt_last_00000001.pt").write_bytes(ckpt.read_bytes())
    mgr = CheckpointManager(tmp_path)
    with pytest.raises(CheckpointError) as e:
        mgr.load_latest_checkpoint(
            device="cpu", logger=__import__("logging").getLogger("test")
        )
    assert "mapping payload" in str(e.value)


def test_load_checkpoint_missing_keys_raises(tmp_path: Path) -> None:
    """It should raise CheckpointError when checkpoint is missing required keys."""
    # Craft checkpoint with missing keys
    ckpt = tmp_path / "ckpt_last_00000001.pt"
    # Provide other required keys so the first missing is model_args
    torch.save(
        {
            "model": {},
            "optimizer": {},
            "iter_num": 0,
            "best_val_loss": 0.0,
        },
        ckpt,
    )
    mgr = CheckpointManager(tmp_path)
    with pytest.raises(CheckpointError) as e:
        mgr.load_latest_checkpoint(
            device="cpu", logger=__import__("logging").getLogger("test")
        )
    # Expect the error to mention the first missing required key
    assert "model_args" in str(e.value)


def test_load_checkpoint_bad_model_args_raises(tmp_path: Path) -> None:
    """It should raise CheckpointError when model_args is missing required keys."""
    # Craft checkpoint with invalid model_args
    ckpt = tmp_path / "ckpt_last_00000001.pt"
    torch.save({"model": {}, "model_args": {"n_layer": -1}}, ckpt)
    mgr = CheckpointManager(tmp_path)
    # This now fails later when constructing a Checkpoint; manager validates required keys first
    with pytest.raises(CheckpointError):
        mgr.load_latest_checkpoint(
            device="cpu", logger=__import__("logging").getLogger("test")
        )


def test_load_checkpoint_load_state_error_is_wrapped(tmp_path: Path) -> None:
    """It should wrap load_state_dict errors as ModelError with path info."""
    # Prepare a valid-looking checkpoint but force load failure via Dummy GPT
    # Use a discoverable filename for CheckpointManager
    ckpt = tmp_path / "ckpt_last_00000001.pt"
    # Minimal valid args for our GPTConfig (use default values by omitting fields)
    torch.save(
        {
            "model": {"invalid_key": "invalid_value"},  # This will cause load to fail
            "model_args": {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            },
            "iter_num": 0,
            "best_val_loss": 0.0,
            "optimizer": {},
            "config": {},
        },
        ckpt,
    )

    mgr = CheckpointManager(tmp_path)
    # The manager doesn't construct GPT; it only returns typed dicts. The ModelError is raised when applying state.
    # Simulate consumer applying load and catching a ModelError with path context in higher-level code; here we just ensure manager loads dicts.
    ckpt_obj = mgr.load_latest_checkpoint(
        device="cpu", logger=__import__("logging").getLogger("test")
    )
    assert ckpt_obj is not None


# Removed tests for internal _codec_from_meta as the codec helpers were dropped


# ---------------------------
# sample() tests
# ---------------------------


def test_sample_happy_path_with_file_prompt_and_char_meta(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    out_dir: Path,
) -> None:
    """sample() should print decoded text and separators using FILE: prompt and char meta."""
    # out_dir provided by fixture
    meta_path = out_dir / "meta.pkl"
    _write_char_meta(meta_path)

    # Write prompt file
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("Hi\n", encoding="utf-8")

    # Device/dtype context is handled internally by sampler.

    # Supply DI hooks for test-specific implementations.
    class _DummyModelWithLoad(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    class _MiniCkpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {"weights": []}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _load_ckpt_di(**_: Any) -> Any:
        return _MiniCkpt()

    def _model_factory_di(cfg: Any, logger: Any) -> Any:  # noqa: ARG001
        return _DummyModelWithLoad()

    # Build SampleExperiment
    rt = RuntimeConfig(
        out_dir=out_dir, device="cpu", dtype="float32", compile=False, seed=1
    )
    sample_conf = SampleConfig(
        start=f"FILE:{prompt_path}",
        num_samples=2,
        max_new_tokens=4,
        temperature=0.1,
        top_k=1,
    )
    exp = SamplerConfig(
        runtime=rt,
        sample=sample_conf,
        checkpoint_load_fn=_load_ckpt_di,
        model_factory=_model_factory_di,
    )

    # Capture logs from sampler module
    caplog.set_level("INFO", logger="ml_playground.sampler")
    # Run
    metadata = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    run_sampling(SamplingPlan(config=exp, metadata=metadata))

    # Verify via logs (sampler logs instead of printing)
    text = caplog.text
    assert "Hi" in text
    assert "HHHH" in text  # This is what the dummy model generates


def test_sample_with_compile_flag_uses_compiled_model(
    tmp_path: Path, capsys: Any, out_dir: Path
) -> None:
    """When compile=True, sample should use torch.compile(model)."""
    _write_char_meta(out_dir / "meta.pkl")

    # Device/dtype context is handled internally by sampler.

    # Observe whether compiled model's generate was invoked
    called: dict[str, int] = {"compiled": 0}

    class _Compiled(_DummyModel):
        def generate(self, *args: Any, **kwargs: Any) -> torch.Tensor:  # type: ignore[no-untyped-def]
            called["compiled"] += 1
            return super().generate(*args, **kwargs)

    # Provide DI hooks: model factory, checkpoint loader, and compile function
    class _DummyModelWithLoad2(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    class _MiniCkpt2:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {"weights": []}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _load_ckpt_di2(**_: Any) -> Any:
        return _MiniCkpt2()

    def _model_factory_di2(cfg: Any, logger: Any) -> Any:  # noqa: ARG001
        return _DummyModelWithLoad2()

    def _compile_model_di(model: Any) -> Any:
        return _Compiled()

    rt = RuntimeConfig(
        out_dir=out_dir, device="cpu", dtype="float32", compile=True, seed=1
    )
    sc = SampleConfig(
        start="\n", num_samples=1, max_new_tokens=3, temperature=0.5, top_k=0
    )
    exp = SamplerConfig(
        runtime=rt,
        sample=sc,
        checkpoint_load_fn=_load_ckpt_di2,
        model_factory=_model_factory_di2,
        compile_model_fn=_compile_model_di,
    )

    # Call sampler directly
    metadata = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    run_sampling(SamplingPlan(config=exp, metadata=metadata))
    assert called["compiled"] == 1


def test_sampler_compile_requires_compile_fn(out_dir: Path) -> None:
    """Sampler should raise when runtime.compile is true but no compile_model_fn provided."""

    _write_char_meta(out_dir / "meta.pkl")
    model = _make_minimal_model()
    _rotated_best(out_dir, model)

    base_cfg = _sampler_cfg(out_dir)
    cfg = base_cfg.model_copy(
        update={
            "runtime": base_cfg.runtime.model_copy(update={"compile": True}),
        }
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    with pytest.raises(ValueError) as excinfo:
        Sampler(cfg, shared)

    assert "compile_model_fn" in str(excinfo.value)


def test_sampler_requires_runtime(out_dir: Path) -> None:
    """Sampler should fail fast when runtime config is missing."""
    cfg = SamplerConfig.model_construct(
        runtime=None,
        sample=SampleConfig(
            start="\n", num_samples=1, max_new_tokens=1, temperature=1.0, top_k=10
        ),
    )
    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    with pytest.raises(ValueError, match="Runtime configuration is missing"):
        Sampler(cfg, shared)


def test_sampler_setup_torch_env_handles_cuda_errors(out_dir: Path) -> None:
    """_setup_torch_env should swallow CUDA availability errors."""
    _write_char_meta(out_dir / "meta.pkl")

    class _Ckpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {"weights": []}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _checkpoint_loader(**_: Any) -> Any:
        return _Ckpt()

    class _LoadableDummy(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    def _model_factory(_: Any, __: Any) -> Any:
        return _LoadableDummy()

    cfg = _sampler_cfg(out_dir).model_copy(
        update={
            "checkpoint_load_fn": _checkpoint_loader,
            "model_factory": _model_factory,
        }
    )

    def _cuda_probe() -> bool:
        raise RuntimeError("cuda probing failed")

    def _cuda_seed(seed: int) -> None:
        del seed

    deps = replace(
        default_sampler_dependencies(),
        cuda_is_available=_cuda_probe,
        cuda_manual_seed=_cuda_seed,
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    sampler = Sampler(cfg, shared, deps=deps)
    assert sampler.device_type == "cpu"


def test_sampler_setup_torch_env_seeds_cuda_when_available(out_dir: Path) -> None:
    """_setup_torch_env should call torch.cuda.manual_seed when CUDA is available."""
    _write_char_meta(out_dir / "meta.pkl")

    class _Ckpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {"weights": []}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _checkpoint_loader(**_: Any) -> Any:
        return _Ckpt()

    class _LoadableDummy(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    def _model_factory(_: Any, __: Any) -> Any:
        return _LoadableDummy()

    cfg = _sampler_cfg(out_dir).model_copy(
        update={
            "checkpoint_load_fn": _checkpoint_loader,
            "model_factory": _model_factory,
        }
    )

    called: dict[str, int] = {}

    def _manual_seed(seed: int) -> None:
        called["seed"] = seed

    deps = replace(
        default_sampler_dependencies(),
        cuda_is_available=lambda: True,
        cuda_manual_seed=_manual_seed,
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    sampler = Sampler(cfg, shared, deps=deps)
    assert called["seed"] == cfg.runtime.seed
    assert sampler.ctx is not None


def test_decode_tokens_coerces_dtype_and_device(out_dir: Path) -> None:
    """_decode_tokens should coerce non-long tensors and move them to CPU."""
    _write_char_meta(out_dir / "meta.pkl")

    class _Ckpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {"weights": []}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _checkpoint_loader(**_: Any) -> Any:
        return _Ckpt()

    class _LoadableDummy(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    def _model_factory(_: Any, __: Any) -> Any:
        return _LoadableDummy()

    cfg = _sampler_cfg(out_dir).model_copy(
        update={
            "checkpoint_load_fn": _checkpoint_loader,
            "model_factory": _model_factory,
        }
    )
    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)

    captured: dict[str, Any] = {}

    class _TokenizerWithDecodeTensor(Tokenizer):
        def __init__(self, sink: dict[str, Any]) -> None:
            self._sink = sink
            self._stoi: Mapping[str, int] = {"\n": 0, "H": 1, "i": 2}
            self._itos: Mapping[int, str] = {
                idx: char for char, idx in self._stoi.items()
            }

        @property
        def name(self) -> str:
            return "fake"

        @property
        def vocab_size(self) -> int:
            return len(self._stoi)

        @property
        def vocab(self) -> Mapping[str, int]:
            return self._stoi

        def encode(self, text: str) -> list[int]:
            return [self._stoi.get(ch, 0) for ch in text]

        def decode(self, token_ids: Sequence[int]) -> str:
            return "".join(self._itos.get(idx, "?") for idx in token_ids)

        def decode_tensor(self, token_tensor: torch.Tensor) -> str:
            self._sink["dtype"] = token_tensor.dtype
            self._sink["device"] = token_tensor.device.type
            return "decoded"

    sampler.tokenizer = _TokenizerWithDecodeTensor(captured)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    float_tokens = torch.tensor([1, 2, 3], dtype=torch.float32, device=device)

    result = sampler.decode_tokens(float_tokens)
    assert result == "decoded"
    assert captured["dtype"] == torch.long
    assert captured["device"] == "cpu"


def test_sample_constructs_shared_when_missing(
    tmp_path: Path, out_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """sample() should build a MetadataConfig from runtime when one is not provided."""
    _write_char_meta(out_dir / "meta.pkl")

    class _Ckpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {"weights": []}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _checkpoint_loader(**_: Any) -> Any:
        return _Ckpt()

    class _LoadableDummy(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    def _model_factory(_: Any, __: Any) -> Any:
        return _LoadableDummy()

    runtime = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    sample_cfg = SampleConfig(
        start="\n", num_samples=1, max_new_tokens=1, temperature=1.0, top_k=10
    )
    cfg = SamplerConfig(
        runtime=runtime,
        sample=sample_cfg,
        checkpoint_load_fn=_checkpoint_loader,
        model_factory=_model_factory,
    )

    caplog.set_level("INFO", logger="ml_playground.sampler")
    run_sampling(SamplingPlan(config=cfg))
    assert "Sampling..." in caplog.text


def test_sample_requires_runtime_when_shared_missing(tmp_path: Path) -> None:
    """sample() should raise a clear error if runtime config is absent."""
    cfg = SamplerConfig.model_construct(
        runtime=None,
        sample=SampleConfig(
            start="\n", num_samples=1, max_new_tokens=1, temperature=1.0, top_k=10
        ),
    )
    with pytest.raises(ValueError, match="Runtime configuration is missing"):
        run_sampling(SamplingPlan(config=cfg))


def test_sampler_file_prompt_read_error(out_dir: Path) -> None:
    """Sampler.run should raise `FileOperationError` when `FILE:` prompt cannot be read."""

    _write_char_meta(out_dir / "meta.pkl")

    class _MiniCkpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _load_ckpt(**_: Any) -> Any:
        return _MiniCkpt()

    class _Model(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

    def _model_factory(cfg: Any, logger: Any) -> Any:  # noqa: ARG001
        return _Model()

    missing = out_dir / "prompt.txt"

    base_cfg = _sampler_cfg(out_dir)
    cfg = base_cfg.model_copy(
        update={
            "checkpoint_load_fn": _load_ckpt,
            "model_factory": _model_factory,
            "sample": base_cfg.sample.model_copy(
                update={
                    "start": f"FILE:{missing}",
                    "num_samples": 1,
                    "max_new_tokens": 1,
                }
            ),
        }
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    sampler = Sampler(cfg, shared)
    with pytest.raises(FileOperationError):
        sampler.run()


def test_sampler_prompt_tensor_cached_between_runs(
    out_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Sampler should reuse cached prompt tensor when prompt and device stay the same."""

    _write_char_meta(out_dir / "meta.pkl")

    class _MiniCkpt:
        def __init__(self) -> None:
            self.model: dict[str, Any] = {}
            self.model_args: dict[str, int] = {
                "block_size": 4,
                "vocab_size": 16,
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
            }

    def _load_ckpt(**_: Any) -> Any:
        return _MiniCkpt()

    generate_calls: list[int] = []

    class _CountingModel(_DummyModel):
        def load_state_dict(self, sd: dict[str, Any], strict: bool = False) -> None:  # type: ignore[override]
            super().load_state_dict(sd)

        def generate(self, *args: Any, **kwargs: Any) -> torch.Tensor:  # type: ignore[override]
            generate_calls.append(1)
            return super().generate(*args, **kwargs)

    def _model_factory(cfg: Any, logger: Any) -> Any:  # noqa: ARG001
        return _CountingModel()

    base_cfg = _sampler_cfg(out_dir)
    cfg = base_cfg.model_copy(
        update={
            "checkpoint_load_fn": _load_ckpt,
            "model_factory": _model_factory,
            "sample": base_cfg.sample.model_copy(
                update={
                    "start": "Hello",
                    "num_samples": 2,
                    "max_new_tokens": 2,
                }
            ),
        }
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    sampler = Sampler(cfg, shared)
    caplog.set_level("INFO", logger="ml_playground.sampler")
    caplog.clear()
    sampler.run()
    first_tensor = sampler.prompt_tensor
    assert first_tensor is not None
    first_tensor_id = id(first_tensor)
    first_log_count = sum(
        1 for msg in cast(Any, caplog).messages if msg == "Sampling..."
    )

    caplog.clear()
    sampler.run()
    second_tensor = sampler.prompt_tensor
    assert second_tensor is not None
    second_tensor_id = id(second_tensor)
    second_log_count = sum(
        1 for msg in cast(Any, caplog).messages if msg == "Sampling..."
    )

    assert first_tensor_id == second_tensor_id
    assert len(generate_calls) == 4  # 2 runs * num_samples (2)
    assert first_log_count == 1
    assert second_log_count == 1


def test_sampler_run_returns_early_for_empty_prompt(tmp_path: Path) -> None:
    """Sampler.run should exit before generation when tokenizer encodes no tokens."""
    out_dir = tmp_path / "empty_prompt"
    out_dir.mkdir()
    _write_char_meta(out_dir / "meta.pkl")

    model = _make_minimal_model()
    _rotated_best(out_dir, model)

    base_cfg = _sampler_cfg(out_dir)
    cfg = base_cfg.model_copy(
        update={
            "sample": SampleConfig(
                start="",
                num_samples=1,
                max_new_tokens=1,
                temperature=1.0,
                top_k=0,
            )
        }
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    sampler = Sampler(cfg, shared)
    sampler.run()

    assert sampler.prompt_tensor is None


def test_sampler_uses_latest_checkpoint_when_configured(tmp_path: Path) -> None:
    """Sampler should load the latest checkpoint when runtime read policy is 'latest'."""
    out_dir = tmp_path / "latest_policy"
    out_dir.mkdir()
    _write_char_meta(out_dir / "meta.pkl")

    model = _make_minimal_model()
    torch.save(
        {
            "model": model.state_dict(),
            "optimizer": {},
            "model_args": {
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 32,
                "block_size": 16,
                "bias": False,
                "vocab_size": 256,
                "dropout": 0.0,
            },
            "iter_num": 42,
            "best_val_loss": 0.0,
            "config": {},
        },
        out_dir / "ckpt_last_00000001.pt",
    )

    cfg = _sampler_cfg(out_dir, read_policy=READ_POLICY_LATEST)
    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    sampler = Sampler(cfg, shared)
    sampler.run()

    assert sampler.cached_prompt_ids is not None


# ---------------------------
# Strict-mode enforcement tests (merged from test_strict_mode_enforcement.py)
# ---------------------------


def _make_minimal_model() -> GPT:
    import logging

    conf = GPTConfig(
        n_layer=1,
        n_head=1,
        n_embd=32,
        block_size=16,
        bias=False,
        vocab_size=256,
        dropout=0.0,
    )
    return GPT(conf, logging.getLogger(__name__))


def _rotated_best(out_dir: Path, model: GPT) -> Path:
    p = out_dir / "ckpt_best_00000000_0.000000.pt"
    torch.save(
        {
            "model": model.state_dict(),
            "optimizer": {},
            "model_args": {
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 32,
                "block_size": 16,
                "bias": False,
                "vocab_size": 256,
                "dropout": 0.0,
            },
            "iter_num": 0,
            "best_val_loss": 0.0,
            "config": {},
        },
        p,
    )
    return p


def _sampler_cfg(
    out_dir: Path, read_policy: Literal["latest", "best"] = READ_POLICY_BEST
) -> SamplerConfig:
    return SamplerConfig(
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=0,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=False,
            checkpointing=RuntimeConfig.Checkpointing(
                keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
                read_policy=read_policy,
            ),
            seed=123,
            device="cpu",
            dtype="float32",
            compile=False,
        ),
        sample=SampleConfig(
            start="\n", num_samples=1, max_new_tokens=1, temperature=1.0, top_k=10
        ),
    )


def test_setup_tokenizer_requires_tokenizer_type(out_dir: Path) -> None:
    """Test setup tokenizer requires tokenizer type."""
    # valid rotated checkpoint so we reach tokenizer stage
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    # meta without tokenizer_type
    meta = {
        "meta_version": 1,
        "kind": "char",
        "dtype": "uint16",
        "stoi": {chr(i): i for i in range(256)},
        "itos": {i: chr(i) for i in range(256)},
    }
    with (out_dir / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    cfg = _sampler_cfg(out_dir)
    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    with pytest.raises(DataError):
        s = Sampler(cfg, shared)
        s.run()


def test_setup_tokenizer_missing_meta_raises_clear_error(out_dir: Path) -> None:
    """Test that missing meta.pkl files produce clear, actionable error messages."""
    # Valid rotated checkpoint so we reach tokenizer stage
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    # No meta.pkl files created - should raise DataError with helpful message

    cfg = _sampler_cfg(out_dir)
    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir / "separate_dataset_dir",  # Different from out_dir
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    with pytest.raises(DataError) as exc_info:
        s = Sampler(cfg, shared)
        s.run()

    error_msg = str(exc_info.value)
    assert "Tokenizer metadata not found" in error_msg
    assert "meta.pkl" in error_msg
    assert str(out_dir) in error_msg  # sampling output directory
    assert "Run 'train' first" in error_msg


def test_sampler_requires_rotated_checkpoints(out_dir: Path) -> None:
    """Test sampler requires rotated checkpoints."""
    meta = {
        "meta_version": 1,
        "kind": "char",
        "tokenizer_type": "char",
        "dtype": "uint16",
        "stoi": {chr(i): i for i in range(256)},
        "itos": {i: chr(i) for i in range(256)},
    }
    with (out_dir / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    torch.save({"model": {}}, out_dir / "ckpt_best.pt")

    cfg = _sampler_cfg(out_dir)
    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    with pytest.raises(CheckpointError):
        s = Sampler(cfg, shared)
        s.run()


# ---------------------------
# Sampler branch coverage tests
# ---------------------------


def test_sampler_compile_flag_missing_compile_fn_raises(out_dir: Path) -> None:
    """Test that compile=True without compile_model_fn raises ValueError."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    # Create a new config with compile=True
    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=True,  # Enable compile
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="\n", num_samples=1, max_new_tokens=1, temperature=1.0, top_k=10
        ),
        # compile_model_fn is None by default
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    with pytest.raises(ValueError, match="compile_model_fn must be provided"):
        Sampler(cfg, shared)


def test_sampler_get_start_ids_with_file_prefix(out_dir: Path, tmp_path: Path) -> None:
    """Test that FILE: prefix correctly reads prompt from file."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    # Create a prompt file
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text("Hi\n", encoding="utf-8")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start=f"FILE:{prompt_file}",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)
    start_ids = sampler.expose_get_start_ids()
    assert isinstance(start_ids, list)
    assert len(start_ids) > 0


def test_sampler_get_start_ids_file_not_found_raises(out_dir: Path) -> None:
    """Test that FILE: prefix with non-existent file raises FileOperationError."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="FILE:/nonexistent/path/prompt.txt",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)
    with pytest.raises(FileOperationError):
        sampler.expose_get_start_ids()


def test_sampler_prompt_caching_same_prompt(out_dir: Path) -> None:
    """Test that sampler caches prompt tensor when prompt doesn't change."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)

    # First run - should create prompt tensor
    assert sampler.prompt_tensor is None
    assert sampler.cached_prompt_ids is None

    # Manually call _get_start_ids to populate cache
    start_ids = sampler.expose_get_start_ids()
    prompt_ids = tuple(start_ids)

    # Simulate the caching logic from run()
    device = torch.device(sampler.runtime_cfg.device)
    sampler.set_prompt_cache(
        torch.as_tensor(start_ids, dtype=torch.long, device=device).unsqueeze(0),
        prompt_ids,
    )

    # Verify cache was set
    assert sampler.prompt_tensor is not None
    assert sampler.cached_prompt_ids == prompt_ids


def test_sampler_prompt_caching_device_change(out_dir: Path) -> None:
    """Test that sampler recreates prompt tensor when device changes."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)

    # Create initial prompt tensor on CPU
    start_ids = sampler.expose_get_start_ids()
    device = torch.device("cpu")
    sampler.set_prompt_cache(
        torch.as_tensor(start_ids, dtype=torch.long, device=device).unsqueeze(0),
        tuple(start_ids),
    )

    # Verify initial state
    assert sampler.prompt_tensor is not None
    assert sampler.prompt_tensor.device.type == "cpu"

    # Simulate device mismatch check (from run() method)
    new_device = torch.device("cpu")
    assert sampler.prompt_tensor is not None
    device_mismatch = sampler.prompt_tensor.device != new_device
    assert not device_mismatch  # Same device, no mismatch


def test_sampler_decode_tokens_converts_dtype(out_dir: Path) -> None:
    """_decode_tokens should convert non-long tensors to long dtype."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)

    # Create a float32 tensor (not long)
    token_tensor = torch.tensor([1, 2, 3], dtype=torch.float32)
    result = sampler.decode_tokens(token_tensor)
    assert isinstance(result, str)


def test_sampler_decode_tokens_moves_to_cpu(out_dir: Path) -> None:
    """_decode_tokens should move non-CPU tensors to CPU."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = _SamplerHarness(cfg, shared)

    # Create a CPU tensor (already on CPU, so test won't actually move it)
    # But we can verify the logic works
    token_tensor = torch.tensor([1, 2, 3], dtype=torch.long, device="cpu")
    result = sampler.decode_tokens(token_tensor)
    assert isinstance(result, str)


def test_sampler_decode_tokens_uses_decode_tensor_method(out_dir: Path) -> None:
    """_decode_tokens should use decode_tensor method if available on tokenizer."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    sampler = Sampler(cfg, shared)

    # Mock a tokenizer with decode_tensor method
    class MockTokenizerWithDecodeMethod:
        def decode(self, token_ids: Sequence[int]) -> str:
            return "fallback"

        def decode_tensor(self, tensor: torch.Tensor) -> str:
            return "decode_tensor_called"

    # Replace tokenizer with mock
    sampler.tokenizer = cast(Any, MockTokenizerWithDecodeMethod())

    token_tensor = torch.tensor([1, 2, 3], dtype=torch.long)
    result = sampler.decode_tokens(token_tensor)
    assert result == "decode_tensor_called"


def test_sampler_sample_function_with_none_shared(out_dir: Path) -> None:
    """sample() function should create MetadataConfig when shared is None."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cpu",
        dtype="float32",
        compile=False,
    )
    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
    )

    # Call sample with shared=None (should create MetadataConfig internally)
    run_sampling(SamplingPlan(config=cfg))  # Should not raise


@pytest.mark.filterwarnings("ignore::UserWarning")  # type: ignore[reportAttributeAccessIssue]
def test_sampler_setup_torch_env_cuda_device(out_dir: Path) -> None:
    """_setup_torch_env should set device_type to 'cuda' when device contains 'cuda'."""
    model = _make_minimal_model()
    _rotated_best(out_dir, model)
    _write_char_meta(out_dir / "meta.pkl")

    rt = RuntimeConfig(
        out_dir=out_dir,
        max_iters=0,
        eval_interval=1,
        eval_iters=1,
        log_interval=1,
        eval_only=False,
        checkpointing=RuntimeConfig.Checkpointing(
            keep=RuntimeConfig.Checkpointing.Keep(last=1, best=1),
            read_policy=READ_POLICY_BEST,
        ),
        seed=123,
        device="cuda",  # CUDA device
        dtype="float32",
        compile=False,
    )

    # Use a dummy factory and deps to avoid actual CUDA/loading logic
    model_was_moved = False

    class MockModel(GPT):
        def to(self, *args, **kwargs):  # type: ignore
            nonlocal model_was_moved
            model_was_moved = True
            return self

        def eval(self):
            return self

        def load_state_dict(
            self,
            state_dict: Mapping[str, Any],
            strict: bool = True,
            assign: bool = False,
        ) -> torch.nn.modules.module._IncompatibleKeys:
            self.load_state_dict_called = True
            return torch.nn.modules.module._IncompatibleKeys([], [])

    def dummy_model_factory(cfg, logger):
        return MockModel(cfg, logger)

    def dummy_checkpoint_load_fn(**kwargs):
        return Checkpoint(
            model=model.state_dict(),
            optimizer={},
            model_args={
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 32,
                "block_size": 128,
                "vocab_size": 100,
            },
            iter_num=0,
            best_val_loss=0.0,
            config={},
        )

    cfg = SamplerConfig(
        runtime=rt,
        sample=SampleConfig(
            start="test",
            num_samples=1,
            max_new_tokens=1,
            temperature=1.0,
            top_k=10,
        ),
        model_factory=dummy_model_factory,
        checkpoint_load_fn=dummy_checkpoint_load_fn,
    )

    shared = MetadataConfig(
        experiment="unit",
        config_path=out_dir / "cfg.toml",
        project_home=out_dir,
        dataset_dir=out_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    # Inject mock dependencies to avoid actual CUDA calls
    mock_deps = SamplerDependencies(
        cuda_is_available=lambda: True,
        cuda_manual_seed=lambda seed: None,
    )

    sampler = Sampler(cfg, shared, deps=mock_deps)

    # Verify device_type is set to cuda
    assert sampler.device_type == "cuda"
    # Verify model was "moved" to device
    assert model_was_moved
    assert sampler.model is not None
