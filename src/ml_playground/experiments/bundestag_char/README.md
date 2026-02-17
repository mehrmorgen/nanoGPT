# Bundestag Char

Character-level language model experiment using GermaParlTEI as the only prepare source.

## Prepare behavior

- `prepare bundestag_char` always resolves GermaParlTEI (`PolMine/GermaParlTEI@main` by default).
- A remote GitHub head SHA check is mandatory for every run.
- Skip only happens when all artifacts exist and `meta.pkl` source fields match the remote head.
- If artifacts exist and freshness changes (or metadata is stale), prepare requires explicit overwrite confirmation.
- Cached raw artifact: `raw/germaparl_cache/<repo>_<ref>.tar.gz`.
- No persistent extracted XML tree is written.

Prepared outputs:

- `datasets/input.txt`
- `datasets/train.bin`
- `datasets/val.bin`
- `datasets/meta.pkl`

`meta.pkl` contract is minimal:

- `meta_version`
- `tokenizer_type`, `tokenizer`
- `vocab_size`, `stoi`, `itos`
- `train_tokens`, `val_tokens`
- `source_head_sha`, `source_repo`, `source_ref`

## Prepare extras

Supported prepare extras (`[prepare.extras]`):

- `dataset_dir_override`
- `germaparl_repo`
- `germaparl_ref`
- `germaparl_cache_dir`
- `germaparl_include_stage`
- `germaparl_include_speaker_attrs`
- `split`

No seed/auto/test transport extras are supported.

## Analyze behavior

`analyze bundestag_char` prefers TensorBoard event files under `out/logs/tb`.
If none are found, it falls back to the LIT integration path.

## Commands

```bash
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml prepare bundestag_char
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml train bundestag_char
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml sample bundestag_char
uv run cli --exp-config src/ml_playground/experiments/bundestag_char/config.toml analyze bundestag_char
```
