import z from "zod";

export class InvalidUpdateDescription extends Error {}

export function applyUpdateDescription(
  old: Record<string, unknown>,
  updateDescription: Record<string, unknown>
): Record<string, unknown> {
  if (Object.keys(updateDescription).length === 0) {
    // No update
    return old;
  }
  // Copy the given object by one level
  const res = { ...old };
  for (const [k, v_] of Object.entries(updateDescription)) {
    const vParsed = z.record(z.string(), z.unknown()).safeParse(v_);
    if (!vParsed.success) {
      throw new InvalidUpdateDescription(`Expected object, got ${typeof v_}`);
    }
    const v = vParsed.data;
    switch (k) {
      case "d":
        for (const nestedKey of Object.keys(v)) {
          delete res[nestedKey];
        }
        break;
      case "t":
        for (const [nestedKey, newSize] of Object.entries(v)) {
          const arr = res[nestedKey];
          if (!Array.isArray(arr)) {
            throw new InvalidUpdateDescription(
              `Expected array, got ${typeof arr}`
            );
          }
          if (typeof newSize !== "number") {
            throw new InvalidUpdateDescription(
              `Expected number, got ${typeof newSize}`
            );
          }
          res[nestedKey] = arr.slice(0, newSize);
        }
        break;
      case "i":
        Object.assign(res, v);
        break;
      default: {
        // Must be an embedded change
        if (!k.startsWith("s")) {
          throw new InvalidUpdateDescription(`Unexpected key: "${k}"`);
        }
        const nestedFieldName = k.slice(1);
        const nestedFieldParsed = z
          .record(z.string(), z.unknown())
          .safeParse(res[nestedFieldName]);
        if (!nestedFieldParsed.success) {
          throw new InvalidUpdateDescription(
            `Expected nested object, got ${typeof res[nestedFieldName]}`
          );
        }
        res[nestedFieldName] = applyUpdateDescription(
          nestedFieldParsed.data,
          v
        );
      }
    }
  }

  return res;
}
