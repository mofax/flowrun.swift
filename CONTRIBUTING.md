# Contributing to FlowRun

## Local development

Use Swift 6.3 or later and run the full test suite from the package root:

```sh
swift test
swift package generate-documentation --target FlowRun --analyze --warnings-as-errors
```

The test suite intentionally exercises the package's public guarantees:

| Area | Tests |
| --- | --- |
| API encoding and public contracts | `APIContractTests` |
| independent runs and step serialization | `ConcurrencyTests` |
| SQLite durability, ownership, and stale-run timeouts | `DurabilityTests` |
| replay after cancellation and compatibility failures | `RecoveryTests` |
| persistence implementation behavior | `PersistenceContractTests` |
| retry timing and terminal errors | `RetryBehaviorTests` |
| ordered checkpoint workflows | `MultiStepWorkflowTests` |

## Compatibility rules

Persisted inputs, completed step outputs, and final outputs are JSON-encoded. Treat a workflow identifier, step IDs, order of `context.step` calls, and their decoded Swift types as a durable compatibility contract.

- Preserve existing identifiers, step order, and decoding behavior when resuming old runs must work.
- Change a workflow identifier or relevant step ID when an incompatible or meaning-changing change should invalidate prior checkpoints.
- Add or update recovery tests whenever a change affects cancellation, replay, persistence, retry behavior, or timeouts.
- Do not claim exactly-once execution. Side-effecting steps must remain idempotent.

## Documentation expectations

- Document every public API change with a DocC comment that explains its behavior, constraints, errors, and concurrency or persistence effects where relevant.
- Add or update the relevant guide in `Sources/FlowRun/FlowRun.docc` when behavior changes beyond a single symbol.
- Keep examples executable in spirit and use stable workflow and step identifiers.
- Update `README.md` when installation, supported platforms, or the primary getting-started path changes.

## Before opening a pull request

- Run `swift test`.
- Review the change for persisted-data compatibility.
- Build the DocC archive and check documentation links and code snippets.
- Add a note to `CHANGELOG.md` for user-visible changes.

GitHub Actions runs the same tests and DocC analysis for every push and pull request.
