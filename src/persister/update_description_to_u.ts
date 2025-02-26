import { Document, UpdateDescription } from "mongodb";
import z from "zod";

function switchTo(u: Document, path: string[]): Document {
  let cur = u;
  for (const c of path) {
    if (!(c in cur)) {
      cur[c] = {};
    }
    cur = cur[c];
  }
  return cur;
}

function getDisambiguatedPath(
  field: string,
  dapaths: Record<string, string[]>
): (string | number)[] {
  return (
    z.array(z.string().or(z.number())).optional().parse(dapaths[field]) ??
    field.split(".")
  );
}

// The storage format here is inspired heavily by the
// undocumented oplog entry format of MongoDB itself.
// An unofficial effort to document it can be found at
// https://github.com/meteor/meteor/blob/7da5b32d7882b510df8aa2002f891fc4e1ae1126/packages/mongo/oplog_v2_converter.js
// Most notably, it does not differentiate between
// updates to existing fields vs insertion of new field,
// as well as array keys (integers) vs string keys.
// Additionally, it supports array truncations as a primitive.
export function updateDescriptionToU(upd: UpdateDescription): Document {
  const res = {};
  const dapaths = upd.disambiguatedPaths ?? {};
  for (const field of upd.removedFields ?? []) {
    const path = getDisambiguatedPath(field, dapaths);
    switchTo(res, [...path.slice(0, -1).map((c) => `s${c}`), "d"])[
      path[path.length - 1]!
    ] = false;
  }
  for (const { field, newSize } of upd.truncatedArrays ?? []) {
    const path = getDisambiguatedPath(field, dapaths);
    switchTo(res, [...path.slice(0, -1).map((c) => `s${c}`), "t"])[
      path[path.length - 1]!
    ] = newSize;
  }
  for (const [field, val] of Object.entries(upd.updatedFields ?? {})) {
    const path = getDisambiguatedPath(field, dapaths);
    switchTo(res, [...path.slice(0, -1).map((c) => `s${c}`), "i"])[
      path[path.length - 1]!
    ] = val;
  }

  return res;
}
