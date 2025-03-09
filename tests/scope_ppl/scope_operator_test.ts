import { strict as assert } from "assert";
import { describe, it } from "node:test";
import { scopeOperator } from "../../src/cea/scope_ppl/scope_operator";
import { InvalidExpression } from "../../src/cea/scope_ppl/invalid_expression";

describe("scopeOperator", () => {
  it("can scope $abs", () => {
    assert.deepStrictEqual(scopeOperator({ $abs: "$a" }, "x", {}), {
      $abs: "$x.a",
    });
  });
  describe("$getField", () => {
    it("can scope the shorthand syntax", () => {
      assert.deepStrictEqual(scopeOperator({ $getField: "a" }, "b", {}), {
        $getField: { $concat: [{ $literal: "b" }, ".", "a"] },
      });
    });
    it("can scope the regular syntax", () => {
      assert.deepStrictEqual(
        scopeOperator(
          {
            $getField: {
              field: "$b",
              input: "$a",
            },
          },
          "x",
          { my_var: true }
        ),
        {
          $getField: {
            field: "$x.b",
            input: "$x.a",
          },
        }
      );
    });
  });
  describe("$let", () => {
    it("can scope", () => {
      assert.deepStrictEqual(
        scopeOperator(
          {
            $let: {
              vars: { x: "$y" },
              in: { $add: ["$$x", "$$y", "$z"] },
            },
          },
          "a",
          { y: true }
        ),
        {
          $let: {
            vars: { x: "$a.y" },
            in: { $add: ["$$x", "$$y", "$a.z"] },
          },
        }
      );
    });
    it("cannot use the variables defined in the vars section in the vars section", () => {
      try {
        scopeOperator(
          {
            $let: {
              vars: { x: "$$x" },
              in: 0,
            },
          },
          "a",
          {}
        );
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "Use of undefined variable: x"
        );
      }
    });
    it("value must be an object", () => {
      try {
        scopeOperator(
          {
            $let: 0,
          },
          "a",
          {}
        );
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "$let only supports an object as its argument"
        );
      }
    });
    it("must include the vars field", () => {
      try {
        scopeOperator(
          {
            $let: { in: 0 },
          },
          "a",
          {}
        );
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "Missing 'vars' parameter to $let"
        );
      }
    });
    it("the vars field must be an object", () => {
      try {
        scopeOperator(
          {
            $let: { vars: 0, in: 0 },
          },
          "a",
          {}
        );
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "invalid parameter: expected an object (vars)"
        );
      }
    });
    it("must include the in field", () => {
      try {
        scopeOperator(
          {
            $let: { vars: {} },
          },
          "a",
          {}
        );
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "Missing 'in' parameter to $let"
        );
      }
    });
  });
  it("can scope $literal", () => {
    // By not scoping
    assert.deepStrictEqual(scopeOperator({ $literal: "$x" }, "a", {}), {
      $literal: "$x",
    });
  });
  it("throws on unsupported operators", () => {
    try {
      scopeOperator({ $lol: 0 }, "a", {});
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidExpression &&
          e.message === "Unrecognized expression: 'lol'"
      );
    }
  });
});
