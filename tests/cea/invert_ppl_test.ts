import { describe, it } from "node:test";
import {
  combineAdjacentMatches,
  invertPipeline,
} from "../../src/cea/invert_ppl";
import { strict as assert } from "assert";

describe("combineAdjacentMatches", () => {
  it("works for empty pipelines", () => {
    assert.deepStrictEqual(combineAdjacentMatches([]), []);
  });
  it("works for a single match stage", () => {
    assert.deepStrictEqual(
      combineAdjacentMatches([{ type: "match", filter: { a: 0 } }]),
      [{ type: "match", filter: { a: 0 } }]
    );
  });
  it("works for a single non-match stage", () => {
    assert.deepStrictEqual(
      combineAdjacentMatches([{ type: "addFields", fields: { a: 0 } }]),
      [{ type: "addFields", fields: { a: 0 } }]
    );
  });
  it("works for two match stages", () => {
    assert.deepStrictEqual(
      combineAdjacentMatches([
        { type: "match", filter: { a: 0 } },
        { type: "match", filter: { b: 0 } },
      ]),
      [{ type: "match", filter: { $and: [{ a: 0 }, { b: 0 }] } }]
    );
  });
  it("works for three match stages", () => {
    assert.deepStrictEqual(
      combineAdjacentMatches([
        { type: "match", filter: { a: 0 } },
        { type: "match", filter: { b: 0 } },
        { type: "match", filter: { c: 0 } },
      ]),
      [{ type: "match", filter: { $and: [{ a: 0 }, { b: 0 }, { c: 0 }] } }]
    );
  });
  it("preserves non-match stages", () => {
    assert.deepStrictEqual(
      combineAdjacentMatches([
        { type: "addFields", fields: { a: 0 } },
        { type: "match", filter: { a: 0 } },
        { type: "match", filter: { b: 0 } },
        { type: "addFields", fields: { b: 0 } },
      ]),
      [
        { type: "addFields", fields: { a: 0 } },
        { type: "match", filter: { $and: [{ a: 0 }, { b: 0 }] } },
        { type: "addFields", fields: { b: 0 } },
      ]
    );
  });
});

describe("invertPipeline", () => {
  it("can invert pipelines with no $match", () => {
    assert.deepStrictEqual(
      invertPipeline([{ type: "addFields", fields: { a: 0 } }]),
      []
    );
  });
  it("can invert pipelines with one $match", () => {
    assert.deepStrictEqual(
      invertPipeline([
        { type: "addFields", fields: { a: "$b" } },
        { type: "match", filter: { a: 0 } },
      ]),
      [
        [
          { type: "addFields", fields: { a: "$b" } },
          { type: "match", filter: { $nor: [{ a: 0 }] } },
        ],
      ]
    );
  });
  it("does not invert pipelines with multiple $match-s", () => {
    assert.deepStrictEqual(
      invertPipeline([
        { type: "match", filter: { a: 0 } },
        { type: "addFields", fields: { a: 0 } },
        { type: "match", filter: { b: 0 } },
      ]),
      [[]]
    );
  });
  it("combines adjacent match stages", () => {
    assert.deepStrictEqual(
      invertPipeline([
        { type: "match", filter: { a: 0 } },
        { type: "match", filter: { b: 0 } },
      ]),
      [[{ type: "match", filter: { $nor: [{ $and: [{ a: 0 }, { b: 0 }] }] } }]]
    );
  });
});
