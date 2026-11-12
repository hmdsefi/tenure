# Contributing to tenure

This page covers how to build and test tenure, the rules `core` follows, and how to write the
commits of a pull request. By contributing, you agree that your contributions are licensed under
the [Mozilla Public License 2.0](../LICENSE), the same license as the project.

## Development setup

You need Go 1.26 or newer and, for linting, [golangci-lint](https://golangci-lint.run/) v2.14.
Run these commands from the repository root before every commit:

```shell
gofmt -l $(git ls-files '*.go')                       # prints nothing
go vet ./...
go test ./...
go test -race ./...
GOWORK=off go mod tidy -diff                          # prints nothing
golangci-lint run ./...
go test -count=1 ./internal/dettest/... ./core/...    # go.mod, imports, headers, determinism, golden digests
```

CI runs the same checks on every pull request, on Linux unless stated otherwise:

- golangci-lint with Go 1.26.0;
- `gofmt`, `go mod tidy -diff`, `go vet` and `go build` with the newest stable Go;
- `go test -race ./...` with Go 1.26 and with the newest stable Go;
- the tests of `internal/dettest` and `core` on Linux and macOS with Go 1.26 and the newest stable
  Go, and the `core` tests and `TestCoreDigests` once more as 32-bit Linux (`GOARCH=386`).

The root module, `github.com/hmdsefi/tenure`, depends only on the standard library: its `go.mod`
has no `require` line and the repository has no root `go.sum`. Code that needs another dependency
goes in a nested module.

Every Go file starts with the license header, followed by a blank line:

```go
// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0
```

## Nested modules

The adapters, the simulations, the examples and the benchmarks each live in a directory with its
own `go.mod`, so their dependencies never reach the root module. `go test ./...` in the root does
not enter them. For each nested module `<dir>`, in directory order, run:

```shell
GOWORK=off go -C <dir> mod tidy -diff                 # not for cmd/tenure
GOWORK=off go -C <dir> vet ./...
GOWORK=off go -C <dir> test -race ./...               # simtest: add -short (full seeds run in CI)
(cd <dir> && GOWORK=off golangci-lint run ./...)
```

`simtest` runs each simulation on many seeds, and a seed must replay the same run every time. So
`simtest` has one more command, which runs every passing seed twice and fails if the two runs
differ:

```shell
FAULTLINE_CHECK_DETERMINISM=1 GOWORK=off go -C simtest test -count=1 ./...
```

No test turns this check on by itself, so state that makes two runs of a seed differ, such as a
package-level counter, fails this command and not the others. `TestTraceReplay`, which compares
two runs of one seed record by record, runs only with the variable set and skips otherwise.

`simtest` also has four planted bugs, which build only with the tag `tenure_mutants`. A plain
`vet` or lint never sees their files, so vet and lint `simtest` once more with the tag:

```shell
GOWORK=off go -C simtest vet -tags tenure_mutants ./...
(cd simtest && GOWORK=off golangci-lint run --build-tags tenure_mutants ./...)
```

Do not run `go test` with the tag: each `TestMutant<Name>` fails by design. `TestMutantsCaught`,
which runs without the tag, builds each planted bug with the tag and checks that it fails.

On every pull request, CI runs `simtest` with `-short` and the race detector, and it runs the
`FAULTLINE_CHECK_DETERMINISM=1` command on Linux and macOS with Go 1.26 and the newest stable
Go. It runs the full seed counts on every push to `main`, and 2000 new seeds for each simulation
every night, each passing seed twice. The goroutine-mode tests of `simtest/bubble` cannot replay
a seed exactly, so they skip under `FAULTLINE_CHECK_DETERMINISM=1` and run without it.

Each nested module requires the root module, and any other module of this repository it uses,
through a `replace` directive that points at that module's directory in your checkout. That is
why `GOWORK=off` works: a nested module builds against your local changes without a `go.work`
file. A local `go.work` is useful to work against a faultline or lockstep checkout next to this
one; it is gitignored, so it is never committed.

`cmd/tenure` is the exception: `go install` rejects a module with `replace` directives, so it
has none. Run its `vet`, `test` and lint commands in a workspace instead of with `GOWORK=off`:
either a local `go.work` that uses every module of the checkout, or a scratch copy of the
repository after `go work init && go work use -r .`.

## Tests against real servers

Some tests of the `k8s`, `postgres`, `etcd` and `examples` modules need a real server. Each one
finds its server through one environment variable and skips when the variable is empty:

| Variable | Value | Read by |
|---|---|---|
| `TENURE_TEST_KUBECONFIG` | path of a kubeconfig file; the tests use its current context | `k8s` tests and `TestSignerKubernetes` in `examples` |
| `TENURE_TEST_POSTGRES` | a PostgreSQL connection URL | `postgres` tests and `TestIndexer` in `examples` |
| `TENURE_TEST_ETCD` | comma-separated etcd client endpoints | `etcd` tests |
| `TENURE_TEST_EXTERNAL` | `1` makes a missing server variable fail the test instead of skipping it; empty or `0` skips | every test that needs a server |

CI sets `TENURE_TEST_EXTERNAL=1`, so a misspelled variable fails the job instead of skipping the
tests. These commands start the same servers CI starts, wait until each one is ready, run the
tests and remove the server.

Kubernetes, on a [kind](https://kind.sigs.k8s.io/) v0.33.0 cluster:

```shell
kind create cluster --name tenure --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --kubeconfig "$HOME/.kube/tenure-kind"
TENURE_TEST_EXTERNAL=1 TENURE_TEST_KUBECONFIG="$HOME/.kube/tenure-kind" GOWORK=off go -C k8s test -race -count=1 ./...
kind delete cluster --name tenure --kubeconfig "$HOME/.kube/tenure-kind"
```

The kubeconfig stays outside the checkout, so it cannot be committed by mistake.

PostgreSQL, in a container (CI tests versions 14.24 and 18.6):

```shell
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=tenure -e POSTGRES_DB=tenure postgres:18.6
until docker exec postgres pg_isready -h 127.0.0.1 -U postgres -d tenure; do sleep 1; done
TENURE_TEST_EXTERNAL=1 TENURE_TEST_POSTGRES='postgres://postgres:tenure@127.0.0.1:5432/tenure?sslmode=disable' GOWORK=off go -C postgres test -race -count=1 ./...
docker rm -f postgres
```

`pg_isready -h 127.0.0.1` succeeds only after the image's first-start initialization, because the
temporary server it runs meanwhile listens on a Unix socket only.

etcd, in a container (CI tests versions v3.5.35 and v3.6.16):

```shell
docker run -d --name etcd -p 2379:2379 quay.io/coreos/etcd:v3.6.16 /usr/local/bin/etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:2379
until curl -fsS http://127.0.0.1:2379/health; do sleep 1; done
TENURE_TEST_EXTERNAL=1 TENURE_TEST_ETCD=http://127.0.0.1:2379 GOWORK=off go -C etcd test -race -count=1 ./...
docker rm -f etcd
```

## Determinism rules for core

`core` gives equal outputs for equal inputs on every platform, so that simulations replay it
exactly. Every package at or below `core/` follows these rules; the code in brackets is the one a
failing check prints:

- [DT001] Imports only `cmp`, `errors`, `fmt`, `hash/fnv`, `math`, `math/bits`, `slices`, `sort`,
  `strconv`, `strings`, `time` and packages under `core/` (the list is `dettest.CoreImports`).
- [DT002] No goroutines, `select` statements or channels.
- [DT003] `time` only for `Time` and `Duration` values and their arithmetic: no clock reads, no
  timers, nothing that reads the local time zone, no `time.Time` passed to `fmt`.
- [DT004] `fmt` only builds strings and errors (`Errorf`, `Sprintf`, `Sprint`, `Sprintln` and the
  `Append` functions); it never prints or reads.
- [DT005] No `range` over a map unless the order cannot reach an output.
- [DT006] No value that depends on a memory address: no pointer, channel or function formatted by
  `fmt`, no `%p`, no `uintptr`, and no `==` on `time.Time` or on a type that contains it.
- [DT007] Every floating-point product is converted explicitly, as in `float64(a*b)`, so no
  platform fuses it with an addition; no complex numbers; only the exactly rounded `math`
  functions.
- [DT008] No `sort.Sort`, `sort.Slice` or `slices.SortFunc` unless the comparison is a total
  order; otherwise use a stable sort, `slices.Sort`, `sort.Ints` or `sort.Strings`.
- [DT009] No writes to package-level variables after their declarations.
- [DT010] No build constraints, no file names that only some platforms compile, and no assembly, C
  or other non-Go source files.

Two rules can be suppressed on one line, with a comment that says why the line is safe. The
comment goes at the end of the line or alone on the line above it:

- `//tenure:maporder <reason>` allows a map range, for example
  `//tenure:maporder keys are collected and sorted below`.
- `//tenure:totalorder <reason>` allows an unstable sort, for example
  `//tenure:totalorder score desc, ID asc`.

A directive without a reason, with an unknown name, or on a line it does not change is itself
reported. `TestCoreImports` checks the import list and `TestCoreDeterminism` checks every other
rule; both run in `go test ./internal/dettest`.

## API snapshots and golden files

`api/tenure.txt` and `api/core.txt` record the exported API of packages `tenure` and `core`. If
you change that API or an exported doc comment, regenerate both files with Go 1.26.0 in the same
commit:

```shell
GOTOOLCHAIN=go1.26.0 GOWORK=off go doc -all github.com/hmdsefi/tenure > api/tenure.txt
GOTOOLCHAIN=go1.26.0 GOWORK=off go doc -all github.com/hmdsefi/tenure/core > api/core.txt
```

Exported doc comments describe behavior in their own words: they never cite a requirement or
acceptance-test ID, and CI fails if one does.

`internal/dettest/testdata/core-digests.txt` pins a hash of the results of `core`'s exported
functions over fixed inputs, so a change to any result fails `TestCoreDigests` on every platform.
An exported `core` function whose result depends on hashing, sorting or arithmetic gets a case in
`internal/dettest/digests_test.go` in the change that adds it. If your change is meant to change
results, rewrite the file instead of editing it by hand:

```shell
GOWORK=off go test ./internal/dettest -run TestCoreDigests -update
```

Then explain the change in a commit message of the pull request with a line such as
`Golden-Update: KeyShard corpus extended`. CI fails a pull request that changes a golden file
without a `Golden-Update: <reason>` line.

## Commits

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/): one of the
types `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore` or `revert`, an
optional lowercase scope, and a short description, for example `feat(core): add KeyShard` or
`ci: cache the module download`. Commit messages carry no AI attribution: no `Co-authored-by:`
line and no "Generated with" line. CI checks every commit of a pull request.

Changes reach `main` only through pull requests. Reference the issue a pull request resolves in
its description, for example `Closes #123`, and say how you tested the change.
