"use strict";
function register(
  app,
  { prefix, known, runtime, operations, fail, validateParams },
) {
  for (const op of operations)
    app.post(prefix + "/" + op.route, known, async (req, res) => {
      const body = req.body === undefined ? {} : req.body;
      if (
        !body ||
        Array.isArray(body) ||
        typeof body !== "object" ||
        Object.keys(body).some((x) => x !== "params") ||
        body.params === null ||
        Array.isArray(body.params) ||
        (body.params !== undefined && typeof body.params !== "object")
      )
        throw fail(400, "INVALID_ARGUMENT", "Expected {params: object}");
      if (
        op.capability !== "-" &&
        !req.vdmDevice.capabilities.includes(op.capability)
      )
        throw fail(
          409,
          "UNSUPPORTED_CAPABILITY",
          `Requires verified ${op.capability} capability`,
        );
      try {
        validateParams(op.method, body.params || {});
      } catch (e) {
        throw fail(400, "INVALID_ARGUMENT", e.message);
      }
      const response = await runtime.call(
        req.params.connectionId,
        req.params.deviceId,
        op,
        body.params || {},
      );
      res
        .status(op.mode === "async" ? 202 : 200)
        .json({
          connectionId: req.params.connectionId,
          deviceId: req.params.deviceId,
          method: op.method,
          status: op.mode === "async" ? "accepted" : "completed",
          response,
        });
    });
}
module.exports = { register };
