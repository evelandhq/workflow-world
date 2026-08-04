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

Proves: the World satisfies upstream's spec in external mode, and the dispatch
contract is enforced (eve's generated flow route calls _this_ package's
`createQueueHandler`, so a wrong runtime secret produces a 401 and a dead
letter — see `dispatch-guard.test.mts`).

Does **not** prove correctness under concurrent delivery for one run. Every test
in upstream's suite is a single sequential invoke, so it cannot see duplicate
step execution. That is `r1.test.mts`'s job, and it is a separate gate.

## Running it

Needs a Postgres and a built `dist/`.

```bash
WORKFLOW_WORLD_CONFORMANCE_URL=postgres://user:pass@127.0.0.1:5432/wfw_conformance npm run test:conformance
```
