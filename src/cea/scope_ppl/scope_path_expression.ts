import { strict as assert } from "assert";
import { InvalidExpression } from "./invalid_expression";

export function scopePathExpression(
  e: string,
  path: string,
  // A set of user-defined variable names to allow
  vars: Record<string, true>
): string | Record<string, unknown> {
  assert(e.startsWith("$"));

  const epath = e.split(".");
  if (epath[0]!.startsWith("$$")) {
    // Variable - see https://www.mongodb.com/docs/manual/reference/aggregation-variables/
    const varName = epath[0]!.substring(2);
    if (vars[varName]) {
      // No need to scope user-defined variables
      return e;
    }
    switch (varName) {
      case "NOW":
      case "CLUSTER_TIME":
      case "REMOVE":
      case "DESCEND":
      case "PRUNE":
      case "KEEP":
        // No need to scope
        return e;
      case "CURRENT":
      case "ROOT":
        // Need to scope
        return [epath[0], path, ...epath.slice(1)].join(".");
      default:
        throw new InvalidExpression(`Use of undefined variable: ${varName}`);
    }
  }
  // Shorthand notation - scope accordingly
  return ["$" + path, epath[0]!.substring(1), ...epath.slice(1)].join(".");
}
