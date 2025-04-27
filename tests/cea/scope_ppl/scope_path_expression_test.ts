import { strict as assert } from "assert";
import { describe, it } from "../../test_utils/tests_polyglot";
import { InvalidExpression } from "../../../src/cea/scope_ppl/invalid_expression";
import { scopePathExpression } from "../../../src/cea/scope_ppl/scope_path_expression";

describe("scopePathExpression", () => {
  it("can scope $$REMOVE", () => {
    assert.deepStrictEqual(
      scopePathExpression("$$REMOVE", "a", {}),
      "$$REMOVE"
    );
  });
  it("can scope $$CURRENT", () => {
    assert.deepStrictEqual(
      scopePathExpression("$$CURRENT.x", "a", {}),
      "$$CURRENT.a.x"
    );
  });
  it("can scope $$ROOT", () => {
    assert.deepStrictEqual(scopePathExpression("$$ROOT", "a", {}), "$$ROOT.a");
  });
  it("can scope shorthand $$CURRENT", () => {
    assert.deepStrictEqual(scopePathExpression("$x.y", "a", {}), "$a.x.y");
    assert.deepStrictEqual(scopePathExpression("$x", "a", {}), "$a.x");
  });
  it("can scope custom variables", () => {
    assert.deepStrictEqual(scopePathExpression("$$a", "a", { a: true }), "$$a");
    assert.deepStrictEqual(
      scopePathExpression("$$a.b", "a", { a: true }),
      "$$a.b"
    );
  });
  it("rejects accesses to unknown variables", () => {
    try {
      scopePathExpression("$$my_var", "x", { var1: true });
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidExpression &&
          e.message === "Use of undefined variable: my_var"
      );
    }
  });
});
