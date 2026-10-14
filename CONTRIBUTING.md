# Contributing to mydb

mydb is a learning project for database storage internals. Keep changes small enough to explain the page format or tree invariant they affect.

## Development setup

Use Go 1.24 or newer, as declared in [go.mod](go.mod). Work in a disposable clone: integration tests create database files in their package directory. Local database and coverage files are ignored by Git.

```bash
go test ./store
go test ./...
```

The first command exercises the storage package; the second includes the integration examples. These commands describe the development workflow, not a claim that every test currently passes.

## Changes to storage behavior

- Start with a minimal reproduction using synthetic records and a fresh database file.
- For tree changes, check insertion, lookup, page splits, and reopening the file. If deletion changes, state which rebalance cases are covered.
- Add a regression test that checks the result, not only that a function returned.
- Explain any change to the on-disk layout and whether old experimental files remain readable.
- Run `gofmt` on changed Go files and include the commands and results you actually verified.

## Documentation changes

Keep the [implementation map](README.md#实现范围) and source links accurate. Distinguish implemented features from ideas; the command parser is a limited experiment rather than a complete SQL engine.

Never include real user data or database dumps in issues or pull requests.
