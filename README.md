# DAGForge

Execute declarative data workflows with SQLite checkpoints and dependency-aware retries.

An executable, dependency-free Python engineering project. Runs locally; no API keys,
cloud account, or paid services are required. Python 3.11+.

## Quick start

From this repository's root:

```sh
python -m dagforge examples/pipeline.json --db runs.db
python -m unittest discover -s tests -v
```

Optional installation: `python -m pip install -e .` exposes the `dagforge` command.

## What it does

JSON defines a DAG of `literal`, `sum`, `product`, and `concat` operations.
Outputs flow to dependents in the declared `deps` order. Every attempt and
successful output is persisted transactionally in SQLite. Re-running the same
workflow resumes successful nodes; changed workflow contents start a new run.
Failures block descendants while independent branches still execute.

## Design boundaries

Single process, sequential scheduler. No shell execution or arbitrary expression eval.
Resume caches successful *pure* operations; it does not promise exactly-once external
side effects. Retries are immediate and bounded. The database is a local checkpoint,
not a distributed queue. Run IDs hash the full JSON workflow, not individual subgraphs.


## Repository map

- `dagforge/core.py`: domain model and algorithms
- `dagforge/__main__.py`: command-line interface
- `tests/`: behavior and regression tests
- `examples/`: reproducible input data
- `DESIGN.md`: tradeoffs and suggested exercises
- `.github/workflows/test.yml`: CI configuration (remote execution requires GitHub Actions)

## Status

Version 1.0 is a complete local demonstration within the documented scope.
It is not a production service. Local tests are reproducible with the command above;
CI status should be checked on the actual repository after publishing.
