# Testing

Comparison of testing approaches and rationale for the choice reflected in this repo’s configuration.

## Comparison (summary)

| Tool/approach     | Fixtures/parametrization | Plugin ecosystem                   | Async support | Determinism               | Notes                             |
| ----------------- | ------------------------ | ---------------------------------- | ------------- | ------------------------- | --------------------------------- |
| **pytest**        | Strong                   | Rich (hypothesis, xdist, coverage) | Yes           | Good with seeded fixtures | Current choice in this repo.      |
| unittest (stdlib) | Basic                    | Limited                            | Limited       | Good                      | Minimal deps, more boilerplate.   |
| nose/nose2        | Legacy                   | Stagnant                           | Limited       | Variable                  | Not recommended for new projects. |

## Property-based testing

| Tool           | Ecosystem/maturity | Ergonomics         | Determinism controls  | Notes                        |
| -------------- | ------------------ | ------------------ | --------------------- | ---------------------------- |
| **Hypothesis** | Mature             | Concise strategies | Seeds and derandomize | Current choice in this repo. |

## Our choice

- **pytest** for primary testing due to fixtures, parametrization, ecosystem, and async support.
- **Hypothesis** for property-based tests to explore wide input spaces with determinism controls.

## When to consider alternatives

- unittest: zero external deps, very small scripts; accept more boilerplate.
- Skip property testing: extremely constrained environments; accept lower input coverage.

## Practices

- Keep tests deterministic (seeded randomness, isolated fs/network, no global state).
- Prefer fixtures over ad-hoc setup; minimize monkeypatching.
- Parametrize to cover edge cases concisely.
- Keep test imports side-effect free; avoid test-only branches in production code.
