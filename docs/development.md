# Development

```sh
pnpm test          # unit suite: engine + store contracts against in-memory fakes, no services needed
pnpm test:redis    # executes the Store's MULTIs against a real redis (see below)
pnpm typecheck && pnpm build
```

weir is one writer: every engine action runs on one queue, and every state transition is
one plain Redis MULTI that carries its event with it. The rule, and its consequences (no
guards, no fences, no Lua), is [DESIGN.md](DESIGN.md#single-writer) "Single writer"; read it
before touching the engine or the store. The unit suite mirrors those MULTIs in an
in-memory fake; only a real redis executes them — run the integration file before touching
`src/store/redis.ts`:

```sh
docker run -d --name weir-test-redis -p 6390:6379 redis:7-alpine
pnpm test:redis
```

End to end against a real node: `examples/regtest-demo.sh` narrates one payment (see the
[quickstart](quickstart-regtest.md)); `examples/regtest-e2e.sh` is the full gate — ten
scripted scenarios (RBF, redirect, reorg → demoted, reorg + double-spend → proven conflicted,
TTL, webhook down, restart mid-flight, `/ready` during catch-up, restart mid-reorg) against
the compose regtest stack, asserting the exact events the catcher receives. It needs docker
compose v2, node on the host, and `.env` with `WEBHOOK_SECRET` and `ADMIN_TOKEN` set; it
recreates the stack (`down -v`) and takes about two minutes.

What is planned next: [ROADMAP.md](ROADMAP.md).
