# External-mode conformance

Runs upstream's `@workflow/world-testing` suite against this World in
`runner: external` — the topology the platform actually ships — with the
dispatcher in the loop and a stub standing in for the host's activation API.

This is the gate that matters. Upstream's suite normally exercises a World whose
queue runner lives inside the executor process; here nothing runs the queue
except the dispatcher, so a green run means the whole out-of-process dispatch
path works: enqueue → graphile claim → activation → held vqs POST → executor.

## How it closes the loop

`world-testing` spawns one executor per test and binds
`Number(process.env.PORT) || 0`, so pinning `PORT` gives every spawned executor
the same address. The stub activation API then returns that constant as
`runtimeInstance.endpointPort`, which is all the dispatcher needs to deliver.
Because the harness passes no env of its own, the World's configuration has to
arrive through the vitest process's environment — it is inherited wholesale by
each spawned executor.

The pinned port is why this project runs single-file, single-fork: two concurrent
executors would collide on bind.

## What it does and does not prove

Proves: the World satisfies upstream's spec in external mode, and every dispatch
in the run went out through the real out-of-process path.

Does **not** prove the dispatch guard. A wrong runtime secret producing a 401,
and the dead letter that follows it, are covered by `src/queue.test.ts` and
`src/dispatcher/dispatch-loop.integration.test.ts` in the default suite. That
eve's own generated flow route reaches _this_ package's `createQueueHandler` is
[e2e-tests/](../e2e-tests/)'s job, since conformance never loads an eve.

Does **not** prove correctness under concurrent delivery for one run either.
Every test in upstream's suite is a single sequential invoke, so it cannot see
duplicate step execution. That is `serialization.test.mts`'s job, and it is a
separate gate in this same project.

## Running it

Needs a Postgres and a built `dist/`.

```bash
WORKFLOW_WORLD_CONFORMANCE_URL=postgres://user:pass@127.0.0.1:5432/wfw_conformance npm run test:conformance
```
