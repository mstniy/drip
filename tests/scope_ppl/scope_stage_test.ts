import { describe, it } from "node:test";
import { strict as assert } from "assert";
import { scopeStage } from "../../src/cea/scope_ppl/scope_stage";
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
  it("rejects invalid stages", () => {
    try {
      scopeStage({ $addFields: {}, $match: {} }, "a");
      throw "must have thrown :(";
    } catch (e) {
      assert(
        e instanceof InvalidStage &&
          e.message ===
            "A pipeline stage specification object must contain exactly one field."
      );
    }

    try {
      scopeStage({ $lol: 0 } as any, "a");
      throw "must have thrown :(";
    } catch (e) {
      assert(
        e instanceof InvalidStage &&
          e.message === "Unrecognized pipeline stage name: '$lol'"
      );
    }
  });
});
