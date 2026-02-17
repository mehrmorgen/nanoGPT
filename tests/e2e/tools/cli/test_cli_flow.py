"""End-to-end CLI workflow tests."""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
from pathlib import Path

import numpy as np


def test_e2e_bundestag_char_flow(tmp_path: Path) -> None:
    """Verify train -> sample flow using pre-prepared bundestag_char artifacts via CLI."""
    # Arrange: Create minimal prepared dataset artifacts and config override.
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir()
    input_file = datasets_dir / "input.txt"
    input_file.write_text("Hello world " * 1000, encoding="utf-8")
    train_tokens = np.asarray([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.uint16)
    val_tokens = np.asarray([1, 2, 3, 4], dtype=np.uint16)
    train_tokens.tofile(datasets_dir / "train.bin")
    val_tokens.tofile(datasets_dir / "val.bin")
    meta_payload = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "tokenizer": "char",
        "vocab_size": 256,
        "stoi": {"H": 1, "e": 2, "l": 3, "o": 4, " ": 5, "w": 6, "r": 7, "d": 8},
        "itos": {1: "H", 2: "e", 3: "l", 4: "o", 5: " ", 6: "w", 7: "r", 8: "d"},
        "train_tokens": int(train_tokens.size),
        "val_tokens": int(val_tokens.size),
        "source_head_sha": "test-sha",
        "source_repo": "PolMine/GermaParlTEI",
        "source_ref": "main",
    }
    with (datasets_dir / "meta.pkl").open("wb") as handle:
        pickle.dump(meta_payload, handle)

    out_dir = tmp_path / "out"

    # Config override: point to tmp_path for data and output, use tiny model
    config_content = f"""
[prepare]
tokenizer_type = "char"
dataset_dir = "{str(datasets_dir)}"

[prepare.extras]
dataset_dir_override = "{str(tmp_path)}"

[training.model]
n_layer = 1
n_head = 1
n_embd = 16
block_size = 16
vocab_size = 256
dropout = 0.0
bias = false

[training.data]
tokenizer = "char"
batch_size = 1
block_size = 16
grad_accum_steps = 1

[training.optim]
learning_rate = 1e-3

[training.schedule]
decay_lr = false
warmup_iters = 0
lr_decay_iters = 0
min_lr = 0.0

[training.runtime]
out_dir = "{str(out_dir)}"
max_iters = 2
eval_interval = 1
eval_iters = 1
log_interval = 1
device = "cpu"
dtype = "float32"
compile = false

[sampling.runtime]
out_dir = "{str(out_dir)}"
device = "cpu"
dtype = "float32"
max_iters = 0

[sampling.sample]
start = "\\n"
num_samples = 1
max_new_tokens = 10
temperature = 0.9
top_k = 50
"""
    config_path = tmp_path / "test_config.toml"
    config_path.write_text(config_content, encoding="utf-8")

    # Run wrapper
    base_cmd = [
        sys.executable,
        "-c",
        "from ml_playground.runtime_cli.main import main_entry; main_entry()",
    ]

    def run_cli_safe(command: str, *args: str) -> None:
        cmd = [
            *base_cmd,
            "--exp-config",
            str(config_path),
            command,
            "bundestag_char",
            *args,
        ]
        env = os.environ.copy()
        # Ensure src is in pythonpath
        src_path = Path(__file__).parents[2] / "src"
        env["PYTHONPATH"] = f"{src_path}:{env.get('PYTHONPATH', '')}"

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        if result.returncode != 0:
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
            raise subprocess.CalledProcessError(
                result.returncode, cmd, output=result.stdout, stderr=result.stderr
            )

    # Act 1: Train
    run_cli_safe("train")
    # Checkpoints might be named differently depending on config (last, best)
    # We expect at least one checkpoint
    assert any(out_dir.glob("*.pt")), f"No checkpoints found in {out_dir}"

    # Act 2: Sample
    run_cli_safe("sample")
