# Contributing to Kest

Run commands from the repository root. Copy `.env.example` to `.env`, replace
its placeholders, and keep `.env` local.

```sh
make doctor
make config
make lint
make test
```

`make check` runs the complete repository check. Unit tests execute in the
locked CyberMarket job image so contributors do not need a host Python virtual
environment. Compose validation covers the core platform and both workload
overlays.

Keep dependencies pointing in one direction:

- `src/kest` may depend on third-party libraries but never a workload.
- `workloads` may depend on `kest`, but workloads never depend on one another.
- DAGs call workload APIs and contain orchestration only.
- Compose, Dockerfiles, and service configuration belong in `deploy/local`.
- Tests mirror importable code under `tests/unit` and use behavior-focused names.

Read configuration at an entry point and pass settings into lower-level
functions. Keep imports free of environment validation and network calls. Make
finite jobs idempotent and give long-running processes explicit signal handling.
Preserve raw events before acknowledging CDC offsets. Publish multi-table output
only after every table and quality gate succeeds.

Use lowercase Python package names, descriptive modules, and a single public
lifecycle vocabulary: `setup`, `seed` or `history`, `batch`, `validate`, and
`down`. Add the corresponding Make targets and workload README commands in the
same change.

Before committing, run `make check`. For changes to a workload lifecycle, also
run its setup, batch, and validation targets against disposable or reviewed local
state. Do not remove Docker volumes as part of a normal test.
