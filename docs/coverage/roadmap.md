# Coverage Roadmap

Last updated: 2025-10-04

## Baseline snapshot

- Overall line coverage (2025-10-04): **82.35%**
  (`make coverage-report` under Python 3.13.5)
- Pre-commit gate: `coverage report --fail-under=81.50`
  (CI variance buffer)
- Coverage data stored under `.cache/coverage/coverage.sqlite`

## Gap analysis

- **High-impact deterministic gaps**
  - `ml_playground/cli.py` (73.62%): CLI option error paths, dataset downloads,
    and project scaffolding flows lack unit coverage.
  - `ml_playground/configuration/loading.py` (80.00%): Config file fallbacks and
    environment-variable overrides currently rely on ad-hoc experimentation.
  - `ml_playground/data_pipeline/preparer.py` (33.66%): File IO and metadata
    generation branches are untested; cover with temp directories and fake
    tokenizer fixtures.
  - Experiment preparers (`bundestag_qwen15b_lora_mps`, `bundestag_tiktoken`,
    `speakger/preparer.py`): Deterministic validation branches are missing.

- **Moderate deterministic gaps**
  - `ml_playground/sampling/runner.py` (80.31%): File-based prompt ingestion and
    compile hooks need targeted mocks.
  - `ml_playground/training/loop/runner.py` (81.08%): Best-checkpoint updates
    and evaluation-only mode are partially covered; expand fake dependency tests.

- **Stochastic or hardware-sensitive gaps**
  - `ml_playground/models/core/inference.py` (56.14%): GPU and AMP toggles need
    deterministic seeds and CPU pathways.
  - `ml_playground/training/ema.py` (40.00%): EMA decay on CUDA should be backed
    by deterministic CPU equivalence tests.

## Milestones

1. **CLI & configuration hardening (target ≥85%)**
   - Add unit tests for CLI argument validation, dataset resolution, and shared
     config hand-off.
   - Cover config loader environment expansion and failure modes.
   - Expected impact: +1.0 to +1.5 percentage points.

2. **Data pipeline stabilization (target ≥60%)**
   - Use pytest `tmp_path` fixtures to test preparer metadata flows end-to-end.
   - Add tokenizer stub fixtures to assert error handling.
   - Expected impact: +1.5 to +2.0 percentage points.

3. **Experiment preparer sweep (target ≥50%)**
   - Write deterministic tests for bundestag and tiktoken preparers using fake
     config payloads.
   - Align documentation for experiment scaffolds.
   - Expected impact: +2.0 percentage points.

4. **Sampling + training loops (target ≥85%)**
   - Extend fakes in `tests/unit/training/loop/test_training_runner.py` and
     sampling runner tests to hit compile hooks and fallback paths.
   - Expected impact: +1.0 percentage point.

5. **Stochastic surfaces (stretch)**
   - Investigate deterministic harnesses for EMA and inference modules (seeded CPU runs and tolerance checks).
   - Expected impact: +1.0 percentage point.

## Threshold management

- After each milestone, rerun `make coverage-report` and update
  `.githooks/.pre-commit-config.yaml` `--fail-under` so it trails the new
  baseline by roughly 0.5 percentage points.
- Record threshold changes and milestone status in `.ldres/tv-tasks.md`.
- Audit CI (Quality Gates workflow) after each raise to confirm remote coverage
  still meets or exceeds the threshold.

## Tracking & ownership

- Primary DRI: Thomas (tv)
- Status updates logged in `.ldres/tv-tasks.md` under `tv-2025-10-03:PR?? · Coverage roadmap towards ~100%`.
- Related initiatives: Regression suite (tv-2025-10-03:PR??) and mutation testing (tv-2025-10-03:PR??).
