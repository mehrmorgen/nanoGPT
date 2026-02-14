# Mutation Testing

Comparison of mutation testing tools and rationale for the choice reflected in this repo’s configuration.

## Comparison (summary)

| Tool           | Coverage depth | Speed    | Ecosystem/maturity | Notes                                   |
| -------------- | -------------- | -------- | ------------------ | --------------------------------------- |
| **Cosmic Ray** | High           | Slow     | Mature             | Current choice in this repo (optional). |
| mutmut         | Moderate       | Moderate | Active             | Simpler workflow, fewer integrations.   |
| None           | N/A            | N/A      | N/A                | Faster cycles but less safety.          |

## Our choice

- **Cosmic Ray** as the optional mutation tester to complement coverage-based tests.

## When to consider alternatives

- mutmut: simpler runs when Cosmic Ray’s orchestration is too heavy.
- None: if mutation testing is out of scope due to time/infra constraints; accept less confidence.

## Practices

- Keep mutation runs optional and targeted (slow by nature).
- Run against critical modules; avoid very slow/highly nondeterministic areas.
- Use seed/config to keep runs reproducible.
