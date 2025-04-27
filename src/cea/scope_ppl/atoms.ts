// See https://www.mongodb.com/docs/manual/reference/bson-types/

export function isComposite(v: unknown): v is object {
  return (
    typeof v === "object" &&
    v !== null &&
    // See https://github.com/mongodb/js-bson/blob/7f2a6d3c1a21de23555c887928f253bf75c36ce8/src/extended_json.ts#L60
    !("_bsontype" in v && typeof v._bsontype === "string") &&
    !(v instanceof Date) &&
    !(v instanceof RegExp)
  );
}
