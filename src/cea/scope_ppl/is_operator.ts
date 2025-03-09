import { isComposite } from "./atoms";
import { InvalidExpression } from "./invalid_expression";

export function isOperator(e: unknown): e is Record<string, unknown> {
  if (
    !isComposite(e) ||
    Array.isArray(e) ||
    e === null ||
    !Object.keys(e).some((e) => e.startsWith("$"))
  ) {
    return false;
  }
  const keys = Object.keys(e);
  if (keys.length !== 1) {
    throw new InvalidExpression(
      "an expression specification must contain exactly one field, the name of the expression."
    );
  }

  // Note that scopeOperator will reject this if the
  // operator is not recognized / is banned.

  return true;
}
