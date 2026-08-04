import { eveChannel } from "eve/channels/eve";
import { localDev, none } from "eve/channels/auth";

export default eveChannel({
  // `none()` keeps the session endpoints reachable from the harness without
  // credentials. This fixture is never deployed; it exists to be driven by a
  // test on loopback.
  auth: [localDev(), none()],
});
