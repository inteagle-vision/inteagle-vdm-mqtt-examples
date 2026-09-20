"use strict";
const crypto = require("node:crypto");
const digest = (x) =>
  crypto.createHash("sha256").update(JSON.stringify(x)).digest("hex");
module.exports = { digest };
