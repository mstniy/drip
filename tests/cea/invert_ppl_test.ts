import { describe, it } from "bun:test";
import { invertPipeline } from "../../src/cea/invert_ppl";
import { strict as assert } from "assert";

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
});
