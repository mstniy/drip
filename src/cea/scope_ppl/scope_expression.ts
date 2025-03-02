import _ from "lodash";
import { scopeOperator } from "./scope_operator";
import { scopePathExpression } from "./scope_path_expression";
import { isOperator } from "./is_operator";

function isPathExpression(e: unknown): e is string {
  return typeof e === "string" && e.startsWith("$");
}

function isArrayExpression(e: unknown): e is unknown[] {
  return Array.isArray(e);
}

function isObjectExpression(e: unknown): e is object {
  return typeof e === "object" && e !== null;
}

export function scopeExpression(
  e: unknown,
  root: string,
  vars: Record<string, true>
): unknown {
  // See https://www.mongodb.com/docs/manual/reference/glossary/#std-term-expression
  if (isOperator(e)) {
    return scopeOperator(e, root, vars);
  } else if (isPathExpression(e)) {
    return scopePathExpression(e, root, vars);
  } else if (isArrayExpression(e)) {
    return e.map((ee) => scopeExpression(ee, root, vars));
  } else if (isObjectExpression(e)) {
    return _.mapValues(e, (v) => scopeExpression(v, root, vars));
  } else {
    // A constant, no need to scope
    return e;
  }
}
