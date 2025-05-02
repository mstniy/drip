import { describe, it } from "node:test";
import { strict as assert } from "assert";
import { InvalidExpression } from "../../../src/cea/scope_ppl/invalid_expression";
import { scopeExpression } from "../../../src/cea/scope_ppl/scope_expression";

describe("scopeExpression", () => {
  it("throws for ambiguous operators", () => {
    try {
      scopeExpression({ $eq: [0, 0], $gt: [0, 0] }, "a", {});
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidExpression &&
          e.message ===
            "an expression specification must contain exactly one field, the name of the expression."
      );
    }
  });
});
