// See https://www.mongodb.com/docs/manual/reference/bson-types/

import { BSONValue } from "bson";

export function isComposite(v: unknown): v is object {
  return (
    typeof v === "object" &&
    v !== null &&
    !(v instanceof BSONValue) &&
    !(v instanceof Date) &&
    !(v instanceof RegExp)
  );
}
