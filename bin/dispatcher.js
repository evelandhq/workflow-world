#!/usr/bin/env node

import("../dist/dispatcher/main.js")
  .then((module) => module.main())
  .catch((error) => {
    console.error("Failed to start the workflow dispatcher:", error);
    process.exit(1);
  });
