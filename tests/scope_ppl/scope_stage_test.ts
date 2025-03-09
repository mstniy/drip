import { describe, it } from "node:test";
import { strict as assert } from "assert";
import { scopeStage, scopeStages } from "../../src/cea/scope_ppl/scope_stage";
import { InvalidStage } from "../../src/cea/scope_ppl/invalid_stage";

describe("scopeStage", () => {
  it("can scope $addFields", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          $addFields: {
            a: "$x",
            b: "$y",
          },
        },
        "c"
      ),
      {
        $addFields: {
          "c.a": "$c.x",
          "c.b": "$c.y",
        },
      }
    );
  });
  it("can scope $replaceRoot", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          $replaceRoot: {
            newRoot: "$a",
          },
        },
        "c"
      ),
      {
        $replaceRoot: {
          newRoot: "$c.a",
        },
      }
    );
  });
  it("can scope $unset", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          $unset: "a",
        },
        "c"
      ),
      {
        $unset: "c.a",
      }
    );
    assert.deepStrictEqual(
      scopeStage(
        {
          $unset: ["a", "b.c"],
        },
        "c"
      ),
      {
        $unset: ["c.a", "c.b.c"],
      }
    );
  });
  describe("$match", () => {
    it("can scope", () => {
      assert.deepStrictEqual(scopeStage({ $match: { a: 0 } }, "b"), {
        $match: { "b.a": 0 },
      });
    });
    it("throws if value not object", () => {
      try {
        scopeStage({ $match: 0 } as any, "a"); // eslint-disable-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-explicit-any
        throw new Error("must have thrown :(");
      } catch (e) {
        assert(
          e instanceof InvalidStage &&
            e.message === "the match filter must be an expression in an object"
        );
      }
    });
  });
  it("rejects invalid stages", () => {
    try {
      scopeStage({ $addFields: {}, $match: {} }, "a");
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidStage &&
          e.message ===
            "A pipeline stage specification object must contain exactly one field."
      );
    }

    try {
      scopeStage({ $lol: 0 } as any, "a"); // eslint-disable-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-explicit-any
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidStage &&
          e.message === "Unrecognized pipeline stage name: '$lol'"
      );
    }
  });
});

describe("scopeStages", () => {
  it("works", () => {
    assert.deepStrictEqual(
      scopeStages([{ $match: { a: 0 } }, { $project: { a: 1 } }], "c"),
      [{ $match: { "c.a": 0 } }, { $project: { "c.a": 1 } }]
    );
  });
});
