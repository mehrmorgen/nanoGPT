# Reproducible Builds Decision Brief

## Why This Exists

Recent work on the coverage badge (`coverage-badge-rebase`, PR #35) exposed gaps in our ability to
recreate identical results between macOS laptops and GitHub Actions. Differences in the
`sample_batch()` path, checkpoint retention heuristics, and tokenizer doubles led to inconsistent
coverage artifacts despite identical source revisions (`5bcb32c`, `258ac39`). The badge remains
deferred in `/.ldres/tv-tasks.md` until we can make deterministic claims.

This brief aggregates what we already learned, key unknowns, and industry practices so we can decide
which investments unlock reproducible workflows for ml_playground.

## Current Signals from the Codebase

- **Tests now model realistic collaborators**: Coverage fixes introduced deterministic tokenizer
  stand-ins (`tests/integration/test_datasets_shakespeare.py`, commit `5bcb32c1`) and filesystem
  scaffolding for LIT integration tests. These changes show that removing monkeypatching improves
  determinism but requires richer fixtures.
- **Checkpoint management still mutates state**: `CheckpointManager` keeps in-memory lists that were
  deduplicated to avoid retention flapping, yet we do not persist the retention policy to disk.
- **Seed usage is uneven**: Experiment configs reference reproducibility seeds, but there is no
  enforced policy for CLI overrides or for PyTorch/CUDA deterministic flags (`tests/conftest.py`
  sets a seed, yet data loaders or sampling paths may still diverge).
- **Environment capture is minimal**: `make quality` surfaces pass/fail only. We do not log Python
  version, UV version, or CPU/GPU capabilities alongside results, making drift difficult to spot.
- **Dependency resolution is implicit**: We rely on `uv` to materialize environments, but no
  lockfile is committed. Contributors may resolve slightly different dependency graphs.

## Decision Areas & Options

### 1. Dependency Capture (uv)

- **Status quo**: No `uv.lock`; developers run `uv sync` locally. _Risk_: transitive packages differ
  across machines.
- **Lockfile enforced (recommended)**: Commit `uv.lock`, require `uv sync --locked`, and add CI check
  ensuring lockfile freshness. Reference: "How to use a uv lockfile for reproducible Python
  environments" (PyDevTools Handbook, 2024).
- **Hybrid**: Maintain lockfile for core dependencies but allow opt-in extras for experiments. Needs
  policy documentation.

### 2. Execution Parity (Local vs CI)

- **Native-only**: Keep relying on macOS/Linux differences. _Observed issue_: badge pipeline diverged
  when `sample_batch()` explored a path unique to Linux.
- **Container or devcontainer**: Provide a Dockerfile/Devcontainer that consumes the lockfile so
  GitHub Actions and local runs share the same base image. Docker documents using
  `SOURCE_DATE_EPOCH` and deterministic timestamps for reproducible builds (Docker Docs, 2024).
- **UV-only parity**: Document exact `uvx` commands and environment variables; cheaper but still
  subject to host OS quirks.

### 3. Coverage & Artifact Determinism

- **Test-level seeding**: Continue seeding fixtures and ensure helper doubles expose deterministic
  metadata (e.g., tokenizer `name`, `vocab_size`). Already partially in place (`5bcb32c1`).
- **Run-level seeding**: Standardize a seed pipeline (Python, NumPy, PyTorch, CUDA, Dataloader) per
  PyTorch reproducibility guidance (PyTorch Docs, 2025). Enforce via helper that raises if the seed
  is unset.
- **Artifact normalization**: Normalize coverage XML/JSON ordering and timestamps; consider storing
  derived badges as deterministic SVG built with fixed timestamps (`SOURCE_DATE_EPOCH`).

### 4. Randomness & Scheduling Policy

- **Ad-hoc**: Each experiment chooses its own RNG story (today's default).
- **Central policy (preferred)**: Provide utilities to request deterministic or stochastic modes.
  PyTorch recommends toggling `torch.backends.cudnn.deterministic` and `benchmark` flags depending on
  needs (PyTorch Docs, 2025). Document how we interpret "deterministic enough" for ml_playground.
- **Hybrid**: Deterministic by default in CI; allow stochastic mode locally via flag.

### 5. Environment Telemetry & Governance

- **Minimal logging**: Keep relying on standard CI output (current state).
- **Metadata artifact**: Emit JSON alongside each `make quality` run capturing Python/UV versions,
  OS, CPU/GPU, `SOURCE_DATE_EPOCH`, git SHA, and lockfile hash. Compare against last known baseline to
  flag drift.
- **Policy enforcement**: Gating merges on telemetry comparison may be overkill initially; consider a
  reporting phase first.

## Open Questions & Unknowns

- How strict do we need determinism? Bitwise equality, or functional parity within tolerances?
- What is the appetite for container-based workflows among contributors?
- How will mutation testing interact with deterministic requirements (nightly vs PR gating)?
- Can we safely backfill lockfiles without breaking ongoing experiment work?

## Recommended Experiments

1. **Lockfile dry run**: Generate `uv lock` from `master`, exercise `uv sync --locked` locally and CI.
2. **Coverage reproducibility spike**: Run `make coverage-test` twice on macOS and once in GitHub
   Actions, comparing artifacts to quantify divergence.
3. **Telemetry prototype**: Capture environment JSON and attach it to a PR run to validate reporting.
4. **RNG policy draft**: Enumerate required seeds and flags, then run smoke tests to ensure no
   regression.

## References

- PyDevTools Handbook — "How to use a uv lockfile for reproducible Python environments", 2024.
- Docker Docs — "Reproducible builds with GitHub Actions", 2024 (SOURCE_DATE_EPOCH guidance).
- PyTorch Docs — "Reproducibility", 2025 (seeding and deterministic algorithm notes).
- Internal: `coverage-badge-rebase` branch commits `5bcb32c1`, `258ac39`; deferred task
  `tv-2025-10-03:PR35` in `/.ldres/tv-tasks.md`.
