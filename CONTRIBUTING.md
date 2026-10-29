# Contributing to Flowkit

Thanks for considering a contribution. This is a small project; the
process here is light but real — read this once and you'll be fine.

## Project layout

```
flowkit/
├── cmd/flowkit/         ← production binary entry point
├── internal/            ← kernel packages (RPC, session, SIP, WebRTC, …)
├── sdk/python/          ← Python SDK (pip-installable as flowkit-sdk)
├── deploy/docker/       ← Dockerfile + docker-compose + nginx config
├── examples/            ← api_explorer (browser acceptance UI)
├── tests/e2e/           ← Playwright browser tests
└── tools/capacity/      ← load-testing harness
```

## Dev environment

Required:

- Go 1.26+
- Python 3.10+ (`make venv` creates `.venv/` for you)
- Docker (only if you want to build/run the container)
- `make`, `git`

One-time setup:

```bash
git clone https://github.com/DeepFlowAI/FlowKit.git
cd FlowKit
make venv sdk-install
go env -w GOPROXY=https://goproxy.io,direct  # only if you're behind a strict proxy
```

Working credentials (optional — `mock` providers work without them):

```bash
cp .env.example .env.local
# fill ALIYUN_ACCESS_KEY_ID, ALIYUN_ACCESS_KEY_SECRET, ALIYUN_NLS_APPKEY,
# LLM_API_KEY if you want kernel llm.chat in API Explorer
```

## Make targets you'll use most

```bash
make build          # compile bin/flowkit
make test           # all Go tests
make test-race      # Go tests under -race + coverage
make sdk-test       # Python SDK pytest
make e2e            # headless browser tests (Playwright)
make soak           # 5-peer WebRTC concurrent soak
make sip-e2e        # SIP inbound + outbound + soak (~35s due to SIP timers)
make compose-up     # docker-compose single instance + jaeger
make compose-multi  # 3 instances + nginx
```

`make help` lists everything.

## How a change ships

1. For significant work, open an issue first describing the problem and
   the proposed design
2. Each commit uses **Conventional Commits** in English:
   - `feat(scope): summary` — new behaviour
   - `fix(scope): summary` — bug fix
   - `chore(scope): summary` — tooling, deps, version bumps
   - `test(scope): summary` — tests only
   - `docs(scope): summary` — documentation only
   - `refactor(scope): summary` — no behaviour change

3. Run `make ci && make sdk-test` before pushing
4. Open a pull request; link the issue in the description

For trivial fixes (typos, docs) a single commit is fine — no issue
needed.

## Code style

- Go: `gofmt -s` + `goimports`. CI runs `go vet` and (when installed)
  `golangci-lint`.
- Python: keep it boring. No tooling enforced beyond `pytest`.
- Tests: every commit that changes behaviour adds a test. We hold to
  ≥70% line coverage on `internal/{rpc, session, store, config,
  media/codec, media/rtp, transport/*}`.

## Architecture rules (read before designing)

- Packages under `internal/` form a strict import DAG — no import
  cycles or reverse edges. New deps must be necessary and justified.
- The `transport.MediaIO` interface is the only audio contract between
  transport and session. WebRTC and SIP each provide their own
  implementation; the kernel never branches on which one it is.
- LLM logic NEVER lives in the kernel. Examples and Python apps own
  that. If you find yourself reaching for an OpenAI client in `internal/`,
  stop.

## Commit / branch conventions

- Direct commits to `main` are fine for now (single-maintainer mode)
- Once a contributor base forms, switch to feature-branch + PR
- Tag releases as `v<major>.<minor>.<patch>`; per-milestone interim tags
  use `v<x>.<y>.<z>-m<N>`
- Never force-push `main` or any release tag

## Testing matrix

| What | Where | Command |
|---|---|---|
| Go unit + integration | `internal/**/*_test.go` | `make test-race` |
| Python SDK unit | `sdk/python/tests/` | `make sdk-test` |
| End-to-end (browser) | `tests/e2e/test_api_explorer.py` | `make e2e` |
| SIP signalling | `internal/transport/sip/*_e2e_test.go` | `make sip-e2e` |

All tests should run in under 60 seconds locally (SIP timer tests take
the longest at ~35s).

## Reporting bugs

Open a GitHub issue with:

- What you did (exact commands or code)
- What you expected
- What actually happened (logs, `go version`, `docker version` if relevant)

For security issues, please email the maintainers privately rather than
opening a public issue.

## License

By contributing you agree your contribution will be licensed under
AGPL-3.0 (the same license as Flowkit itself).
