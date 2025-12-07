"""End-to-end CLI workflow tests."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


import pytest


def test_e2e_bundestag_char_flow(tmp_path: Path) -> None:
    """Verify prepare -> train -> sample flow using bundestag_char experiment via CLI."""
    # Arrange: Create input data and config override
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir()
    input_file = datasets_dir / "input.txt"
    input_file.write_text("Hello world " * 1000, encoding="utf-8")

    out_dir = tmp_path / "out"

    # Config override: point to tmp_path for data and output, use tiny model
    config_content = f"""
[prepare]
raw_text_path = "{str(input_file)}"
tokenizer_type = "char"
dataset_dir = "{str(datasets_dir)}"

[prepare.extras]
dataset_dir_override = "{str(tmp_path)}"

[train.model]
n_layer = 1
n_head = 1
n_embd = 16
block_size = 16
vocab_size = 256
dropout = 0.0
bias = false

[train.data]
tokenizer = "char"
batch_size = 1
block_size = 16
grad_accum_steps = 1

[train.optim]
learning_rate = 1e-3

[train.schedule]
decay_lr = false
warmup_iters = 0
lr_decay_iters = 0
min_lr = 0.0

[train.runtime]
out_dir = "{str(out_dir)}"
max_iters = 2
eval_interval = 1
eval_iters = 1
log_interval = 1
device = "cpu"
dtype = "float32"
compile = false

[sample.runtime]
out_dir = "{str(out_dir)}"
device = "cpu"
dtype = "float32"
max_iters = 0

[sample.sample]
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
        "from ml_playground.runtime.cli.main import main_entry; main_entry()",
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

    # Act 1: Prepare
    run_cli_safe("prepare")
    assert (datasets_dir / "train.bin").exists()
    assert (datasets_dir / "meta.pkl").exists()

    # Act 2: Train
    run_cli_safe("train")
    # Checkpoints might be named differently depending on config (last, best)
    # We expect at least one checkpoint
    assert any(out_dir.glob("*.pt")), f"No checkpoints found in {out_dir}"

    # Act 3: Sample
    run_cli_safe("sample")
