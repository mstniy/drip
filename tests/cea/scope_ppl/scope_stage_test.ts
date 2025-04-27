import { describe, it } from "bun:test";
import { strict as assert } from "assert";
import {
  scopeStage,
  scopeStages,
} from "../../../src/cea/scope_ppl/scope_stage";

describe("scopeStage", () => {
  it("can scope $addFields", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          type: "addFields",
          fields: {
            a: "$x",
            b: "$y",
          },
        },
        "c"
      ),
      {
        type: "addFields",
        fields: {
          "c.a": "$c.x",
          "c.b": "$c.y",
        },
      }
    );
  });
  it("can scope $project", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          type: "project",
          fields: {
            a: "$x",
            b: "$y",
          },
        },
        "c"
      ),
      {
        type: "project",
        fields: {
          "c.a": "$c.x",
          "c.b": "$c.y",
        },
      }
    );
  });
  it("can scope $set", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          type: "set",
          fields: {
            a: "$x",
            b: "$y",
          },
        },
        "c"
      ),
      {
        type: "set",
        fields: {
          "c.a": "$c.x",
          "c.b": "$c.y",
        },
      }
    );
  });
  it("can scope $replaceRoot", () => {
    assert.deepStrictEqual(
      scopeStage({ type: "replaceRoot", newRoot: "$a" }, "c"),
      {
        type: "set",
        fields: {
          c: "$c.a",
        },
      }
    );
  });
  it("can scope $replaceWith", () => {
    assert.deepStrictEqual(
      scopeStage({ type: "replaceWith", expr: "$a" }, "c"),
      {
        type: "set",
        fields: {
          c: "$c.a",
        },
      }
    );
  });
  it("can scope $unset", () => {
    assert.deepStrictEqual(
      scopeStage(
        {
          type: "unset",
          fields: "a",
        },
        "c"
      ),
      {
        type: "unset",
        fields: "c.a",
      }
    );
    assert.deepStrictEqual(
      scopeStage(
        {
          type: "unset",
          fields: ["a", "b.c"],
        },
        "c"
      ),
      {
        type: "unset",
        fields: ["c.a", "c.b.c"],
      }
    );
  });
  describe("$match", () => {
    it("can scope", () => {
      assert.deepStrictEqual(
        scopeStage({ type: "match", filter: { a: 0 } }, "b"),
        {
          type: "match",
          filter: { "b.a": 0 },
        }
      );
    });
  });
});

describe("scopeStages", () => {
  it("works", () => {
    assert.deepStrictEqual(
      scopeStages(
        [
          { type: "match", filter: { a: 0 } },
          { type: "project", fields: { a: 1 } },
        ],
        "c"
      ),
      [
        { type: "match", filter: { "c.a": 0 } },
        { type: "project", fields: { "c.a": 1 } },
      ]
    );
  });
});
