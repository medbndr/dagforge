# Design notes

## Architecture

Kahn topological sorting validates the graph before execution. A SHA-256 of canonical JSON isolates checkpoints per workflow. SQLite transactions pair each successful result with its audit record.

## Tradeoffs

All operations run sequentially. Cache invalidation covers the entire workflow and does not include interpreter version or custom runner implementation. Use a new database after changing operation semantics.

## Understanding and extending the project

1. Explain why a persisted result is not exactly-once delivery.
2. Query the attempts table after a retry.
3. Add a pure mean operation with empty-input behavior and tests.

## Engineering provenance

This repository was prepared with AI-assisted implementation and documentation.
Review the code, run the tests, and make your own extensions before describing
particular design decisions as your independent work. No production deployment,
client usage, or historical development activity is claimed.
