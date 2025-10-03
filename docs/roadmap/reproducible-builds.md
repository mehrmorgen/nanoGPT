# Reproducible Builds Epic

## Problem Statement

Although our current workflow enforces quality gates via `make quality`, repeated runs of the
full suite can yield non-deterministic outcomes. Variance stems from sources such as RNG
initialization, sampling order, dependency resolution, and environment drift between local
machines and CI runners. This reduces trust in coverage metrics, complicates triage, and allows
regressions to slip past in apparently "green" pipelines.

## Goals

- Capture an auditable record of dependencies, environment variables, and toolchain versions for
every run across local development and CI.
- Ensure repeated executions of `make quality` (and subordinate targets) produce bitwise-stable
artifacts, including coverage reports, badge outputs, mutation logs, and checkpoints.
- Standardize containerized and local execution paths so CI accurately mirrors developer
workstations.
- Provide fast feedback on drift, flagging non-deterministic outcomes in CI dashboards.

## Scope & Deliverables

1. **Lockfiles and Dependency Capture**
   - Adopt UV's lockfile support end-to-end (`uv.lock`) and ensure Docker/CI pulls from the same
     resolved versions.
   - Store lockfiles in version control and refresh them via scripted flows to capture updates.

2. **Deterministic Coverage Pipeline**
   - Audit the coverage generation steps and remove sources of randomness (e.g., seed RNG,
     freeze test traversal order).
   - Produce a reproducible badge artifact by standardizing coverage data post-processing.

3. **Container Parity & Tooling**
   - Produce a canonical container image or devcontainer definition aligned with the lockfile.
   - Document instructions for local use of the image to run the full suite identically to CI.

4. **Environment Logging**
   - Capture structured metadata (Python version, UV version, OS, GPU availability, environment
     variables) for each run.
   - Expose metadata via CLI flag or config file for future automation.

5. **Randomness Policy**
   - Define guidance for RNG seeding and state management across training, sampling, and evaluation
     scripts.
   - Bake seed management into templates and CLI parameters.

6. **Governance & Guardrails**
   - Establish CI checks comparing current run metadata against a known-good snapshot.
   - Provide developer documentation on the reproducible workflow and expectations prior to
     merging branches.

## Risks & Mitigations

- **Tooling Drift**: Lockfiles may fall out of sync if manual upgrades bypass scripted flows.
  _Mitigation_: Add CI check ensuring lockfile hash matches `uv` resolution and document refresher
  workflow.

- **Performance Impact**: Additional logging and checks could slow down CI.
  _Mitigation_: Perform deterministic coverage only on scheduled/nightly jobs, keeping smoke suites
  fast.

- **Adoption Complexity**: Developers may resist container workflows.
  _Mitigation_: Provide fallback scripts using native UV commands and clear quick-start docs.

- **Seed Management**: Enforced RNG seeding could hide legitimate stochastic bugs.
  _Mitigation_: Offer opt-out flags with documented rationale and log when non-deterministic mode is
  requested.

## Acceptance Criteria

- Lockfiles (`uv.lock`, container definitions) are versioned and referenced by both local and CI
  tooling.
- `make quality` produces identical coverage outputs across repeated runs on CI.
- Metadata capture is automatically collected and surfaced in CI logs or artifacts.
- A documented checklist exists for new experiments covering seed policy and environment capture.
- QA sign-off on container parity by running the suite locally and in CI with matching results.

## Next Steps

1. Roadmap review with team stakeholders; gather feedback and adjust priorities.
2. Draft implementation tickets for each scoped deliverable.
3. Update `.ldres/tv-tasks.md` with roadmap alignment and cross-links.
4. Begin lockfile enforcement work once roadmap is approved.
