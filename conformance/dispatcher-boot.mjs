/**
 * Boots the real dispatcher for the conformance run.
 *
 * Deliberately `main()` from the built package rather than a hand-assembled
 * runtime: config resolution, the pool/concurrency invariant, the runtime-secret
 * fail-closed rule, boot recovery, the readiness token and the signal handling
 * are all things the gate should be exercising. An inlined boot tests a topology
 * that never ships.
 */
import { main } from "../dist/dispatcher/index.js";

await main();
