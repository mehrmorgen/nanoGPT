from __future__ import annotations

from pathlib import Path

from ml_playground.runtime_cli.main import main


def _write_exp_config(tmp_dir: Path, out_dir: Path, dataset_dir: Path) -> Path:
    dataset_dir_str = str(dataset_dir).replace("\\", "\\\\")
    out_dir_str = str(out_dir).replace("\\", "\\\\")

    content = f'''
[prepare]
dataset_dir = "{dataset_dir_str}"
raw_dir = "{dataset_dir_str}"
raw_text_path = "{dataset_dir_str}/input.txt"
tokenizer_type = "char"
doc_separator = "\\n"

[training.model]
n_layer = 1
n_head = 1
n_embd = 32
block_size = 32
dropout = 0.0
bias = false
vocab_size = 1

[training.data]
batch_size = 2
block_size = 16
grad_accum_steps = 1
sampler = "random"

[training.optim]
learning_rate = 0.0005
weight_decay = 0.0
beta1 = 0.9
beta2 = 0.95
grad_clip = 0.0

[training.schedule]
decay_lr = false
warmup_iters = 0
lr_decay_iters = 1
min_lr = 0.00001

[training.runtime]
out_dir = "{out_dir_str}"
max_iters = 4
eval_interval = 1
eval_iters = 1
log_interval = 1
eval_only = false
always_save_checkpoint = true
seed = 1
device = "cpu"
dtype = "float32"
compile = false
ckpt_write_metadata = false
ckpt_time_interval_minutes = 0
ckpt_atomic = false
best_smoothing_alpha = 0.0
early_stop_patience = 0
ema_decay = 0.0

[training.runtime.checkpointing.keep]
last = 1
best = 1

[sampling.runtime]
out_dir = "{out_dir_str}"
device = "cpu"
dtype = "float32"
compile = false
eval_only = false
always_save_checkpoint = false
seed = 1
max_iters = 0
eval_interval = 1
eval_iters = 1
log_interval = 1

[sampling.sample]
start = "A"
num_samples = 1
max_new_tokens = 8
temperature = 0.1
top_k = 1
'''

    path = tmp_dir / "copy_stage0_test_config.toml"
    path.write_text(content, encoding="utf-8")
    return path


def test_e2e_copy_stage0_prepare_train_sample(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    dataset_dir = tmp_path / "datasets"
    cfg = _write_exp_config(tmp_path, out_dir, dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "input.txt").write_text("A" * 640, encoding="utf-8")

    main(["--exp-config", str(cfg), "prepare", "copy_stage0"])

    assert (dataset_dir / "train.bin").exists()
    assert (dataset_dir / "val.bin").exists()
    assert (dataset_dir / "meta.pkl").exists()

    main(["--exp-config", str(cfg), "train", "copy_stage0"])

    assert any(out_dir.glob("ckpt_last_*.pt"))
    assert any(out_dir.glob("*.pt"))
    assert (out_dir / "meta.pkl").exists()

    main(["--exp-config", str(cfg), "sample", "copy_stage0"])
