import { describe, it } from "node:test";
import { parsePipeline } from "../../../src/cea/parse_ppl/parse_pipeline";
import { strict as assert } from "assert";
import { InvalidStage } from "../../../src/cea/parse_ppl/invalid_stage";

describe("parsePipeline", () => {
  it("rejects invalid stages", () => {
    try {
      parsePipeline([{ $addFields: {}, $match: {} }]);
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof InvalidStage &&
          e.message ===
            "A pipeline stage specification object must contain exactly one field."
      );
    }
  });
  it("can parse $redact", () => {
    assert.deepStrictEqual(parsePipeline([{ $redact: 0 }]), [
      { type: "redact", expr: 0 },
    ]);
  });
  it("can parse $replaceRoot", () => {
    assert.deepStrictEqual(parsePipeline([{ $replaceRoot: { newRoot: 0 } }]), [
      { type: "replaceRoot", newRoot: 0 },
    ]);
  });
  it("can parse $replaceWith", () => {
    assert.deepStrictEqual(parsePipeline([{ $replaceWith: 0 }]), [
      { type: "replaceWith", expr: 0 },
    ]);
  });
  it("can parse $addFields", () => {
    assert.deepStrictEqual(parsePipeline([{ $addFields: { a: 0 } }]), [
      { type: "addFields", fields: { a: 0 } },
    ]);
  });
  it("can parse $project", () => {
    assert.deepStrictEqual(parsePipeline([{ $project: { a: 0 } }]), [
      { type: "project", fields: { a: 0 } },
    ]);
  });
  it("can parse $set", () => {
    assert.deepStrictEqual(parsePipeline([{ $set: { a: 0 } }]), [
      { type: "set", fields: { a: 0 } },
    ]);
  });
  it("can parse $match", () => {
    assert.deepStrictEqual(parsePipeline([{ $match: { a: 0 } }]), [
      { type: "match", filter: { a: 0 } },
    ]);
  });
  it("can parse $unset", () => {
    assert.deepStrictEqual(parsePipeline([{ $unset: "a" }]), [
      { type: "unset", fields: "a" },
    ]);
  });
});
