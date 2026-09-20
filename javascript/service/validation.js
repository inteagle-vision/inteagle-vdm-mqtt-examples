"use strict";
// Wire-specific field/enum validation remains in the codec/device; these shared
// business limits reject common destructive mistakes before publication.
function validateParams(method, p) {
  const integer = (v, min, max, name) => {
    if (!Number.isInteger(v) || v < min || v > max)
      throw new Error(`${name} out of range`);
  };
  if (["initRefTargets", "addTargets", "setTargets"].includes(method)) {
    if (
      !Array.isArray(p.targets) ||
      p.targets.length < 1 ||
      p.targets.length > 128
    )
      throw new Error("targets requires 1..128 entries");
    const ids = p.targets.filter((x) => x.targetId).map((x) => x.targetId);
    if (new Set(ids).size !== ids.length)
      throw new Error("duplicate target ID");
  }
  if (
    method === "deleteTargets" &&
    (!Array.isArray(p.targetIds) ||
      p.targetIds.length < 1 ||
      p.targetIds.length > 128 ||
      new Set(p.targetIds).size !== p.targetIds.length)
  )
    throw new Error("targetIds requires 1..128 unique IDs");
  if (method === "setAttr" && p.sampleFrequencyHz !== undefined)
    integer(p.sampleFrequencyHz, 1, 60, "sampleFrequencyHz");
  if (method === "setLightLevel" && p.allLightsLevel !== undefined)
    integer(p.allLightsLevel, 0, 8, "allLightsLevel");
  if (
    ["getEvidenceStatus", "retryEvidence", "ackEvidencePackage"].includes(
      method,
    ) &&
    (!/^[1-9][0-9]*$/.test(String(p.eventId)) ||
      BigInt(p.eventId) > 0xffffffffffffffffn)
  )
    throw new Error("eventId must be nonzero decimal uint64");
  for (const key of ["page", "pageSize", "limit"])
    if (p[key] !== undefined)
      integer(p[key], 1, key === "page" ? 1000000 : 1000, key);
}
module.exports = { validateParams };
