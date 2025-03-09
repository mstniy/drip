// The "query" here refers to the MQL.
// See https://www.mongodb.com/docs/manual/reference/operator/query/

import { InvalidExpression } from "./invalid_expression";
import { scopeExpression } from "./scope_expression";

function isObjectArray(v: unknown[]): v is Record<string, unknown>[] {
  return !v.some((x) => typeof x !== "object" || x === null);
}

export function scopeQueryClause(
  c: Record<string, unknown>,
  root: string
): Record<string, unknown> {
  const res: Record<string, unknown> = {};

  for (const [k, v] of Object.entries(c)) {
    if (!k.startsWith("$")) {
      // field name - scope
      res[`${root}.${k}`] = scopeQueryPredicate(v, root);
    } else {
      // a query selector
      switch (k) {
        case "$and":
        case "$nor":
        case "$or":
          if (!Array.isArray(v)) {
            throw new InvalidExpression(`${k} argument must be an array`);
          }
          if (!isObjectArray(v)) {
            throw new InvalidExpression(
              `${k} argument's entries must be objects`
            );
          }
          res[k] = v.map((c) => scopeQueryClause(c, root));
          break;
        case "$expr":
          res[k] = scopeExpression(res[k], root, {});
          break;
        default:
          throw new InvalidExpression(
            `unknown top level operator: ${k}. If you have a field name that starts with a '$' symbol, consider using $getField or $setField.`
          );
      }
    }
  }

  return res;
}

// TODO: how about mongo-recognized classes like
// regex, uuid, timestamp, ...
// they'll appear as objects, but must not be recursed into
export function scopeQueryPredicate(p: unknown, root: string): unknown {
  if (typeof p !== "object" || p === null) {
    // shorthand notation for equality/regex match
    // can pass as-is
    return p;
  }
  // a query selector
  // We don't really need to change them, but
  // we do need to validate them.
  const res: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(p)) {
    if (!k.startsWith("$")) {
      // No need to scope the key - scopeQueryClause already did it
      res[k] = scopeQueryPredicate(v, root);
    } else {
      // an operator - validate
      switch (k) {
        case "$eq":
        case "$gt":
        case "$gte":
        case "$in":
        case "$lt":
        case "$lte":
        case "$ne":
        case "$nin":
        case "$not":
        case "$exists":
        case "$type":
        case "$mod":
        case "$regex":
        case "$geoIntersects":
        case "$geoWithin":
        case "$near":
        case "$nearSphere":
        case "$all":
        case "$size":
        case "$bitsAllClear":
        case "$bitsAllSet":
        case "$bitsAnyClear":
        case "$bitsAnySet":
          // can pass as-is
          res[k] = v;
          break;
        case "$elemMatch":
          if (typeof v !== "object" || v === null) {
            throw new InvalidExpression("$elemMatch needs an Object");
          }
          // must recurse
          res[k] = scopeQueryPredicate(v, root);
          break;
        default:
          throw new InvalidExpression(`unknown operator: ${k}`);
      }
    }
  }
  return res;
}
