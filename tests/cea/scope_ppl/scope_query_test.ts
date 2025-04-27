import { strict as assert } from "assert";
import { describe, it } from "../../test_utils/tests_polyglot";
import { InvalidExpression } from "../../../src/cea/scope_ppl/invalid_expression";
import {
  scopeQueryClause,
  scopeQueryPredicate,
} from "../../../src/cea/scope_ppl/scope_query";

describe("scopeQueryClause", () => {
  it("can scope field names", () => {
    assert.deepStrictEqual(
      scopeQueryClause(
        {
          a: 0,
        },
        "b"
      ),
      { "b.a": 0 }
    );
  });
  describe("$and", () => {
    it("can scope", () => {
      assert.deepStrictEqual(
        scopeQueryClause({ $and: [{ a: 0 }, { b: 0 }] }, "c"),
        { $and: [{ "c.a": 0 }, { "c.b": 0 }] }
      );
    });
    it("throws if not array", () => {
      try {
        scopeQueryClause({ $and: 0 }, "a");
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "$and argument must be an array"
        );
      }
    });
    it("throws if array element not object", () => {
      try {
        scopeQueryClause({ $and: [0] }, "a");
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "$and argument's entries must be objects"
        );
      }
    });
  });
  it("can scope $expr", () => {
    assert.deepStrictEqual(
      scopeQueryClause({ $expr: { $eq: ["$a", "$b"] } }, "c"),
      { $expr: { $eq: ["$c.a", "$c.b"] } }
    );
  });
  it("throws for unknown operators", () => {
    try {
      scopeQueryClause({ $hey: 0 }, "a");
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidExpression &&
          e.message ===
            "unknown top level operator: $hey. If you have a field name that starts with a '$' symbol, consider using $getField or $setField."
      );
    }
  });
  it("can scope multiple at once", () => {
    assert.deepStrictEqual(
      scopeQueryClause(
        {
          a: 0,
          $and: [{ b: 0 }],
          $expr: { $ne: [0, "$a"] },
        },
        "c"
      ),
      {
        "c.a": 0,
        $and: [{ "c.b": 0 }],
        $expr: { $ne: [0, "$c.a"] },
      }
    );
  });
});

describe("scopeQueryPredicate", () => {
  it("does not scope the shorthand equality notation", () => {
    assert.strictEqual(scopeQueryPredicate(0, "a"), 0);
  });
  it("does not scope nested keys", () => {
    assert.deepStrictEqual(scopeQueryPredicate({ a: 0 }, "c"), {
      a: 0,
    });
  });
  it("works for $eq", () => {
    assert.deepStrictEqual(scopeQueryPredicate({ $eq: 0 }, "a"), { $eq: 0 });
  });
  describe("$elemMatch", () => {
    it("works", () => {
      assert.deepStrictEqual(
        scopeQueryPredicate({ $elemMatch: { $gt: 0, $lt: 100 } }, "a"),
        { $elemMatch: { $gt: 0, $lt: 100 } }
      );
    });
    it("throws if value not an object", () => {
      try {
        scopeQueryPredicate({ $elemMatch: 0 }, "a");
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidExpression &&
            e.message === "$elemMatch needs an Object"
        );
      }
    });
  });
  it("throws for unknown operators", () => {
    try {
      scopeQueryPredicate({ $hey: 0 }, "a");
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidExpression && e.message === "unknown operator: $hey"
      );
    }
  });
});
