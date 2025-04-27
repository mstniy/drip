import { describe, it } from "../../test_utils/tests_polyglot";
import { parsePipeline } from "../../../src/cea/parse_ppl/parse_pipeline";
import { strict as assert } from "assert";
import { InvalidStage } from "../../../src/cea/parse_ppl/invalid_stage";
import { DripPipelineStageParsed } from "../../../src/cea/parse_ppl/drip_pipeline_stage_parsed";
import { DripPipelineStage } from "../../../src/drip_pipeline";
import { synthPipeline } from "../../../src/cea/parse_ppl/synth_pipeline";

const pairs = [
  [[{ $replaceRoot: { newRoot: 0 } }], [{ type: "replaceRoot", newRoot: 0 }]],
  [[{ $replaceWith: 0 }], [{ type: "replaceWith", expr: 0 }]],
  [[{ $addFields: { a: 0 } }], [{ type: "addFields", fields: { a: 0 } }]],
  [[{ $project: { a: 0 } }], [{ type: "project", fields: { a: 0 } }]],
  [[{ $set: { a: 0 } }], [{ type: "set", fields: { a: 0 } }]],
  [[{ $match: { a: 0 } }], [{ type: "match", filter: { a: 0 } }]],
  [[{ $unset: "a" }], [{ type: "unset", fields: "a" }]],
] as [DripPipelineStage[], DripPipelineStageParsed[]][];

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
  it("can parse pipelines", () => {
    for (const [raw, parsedExpectation] of pairs) {
      assert.deepStrictEqual(parsePipeline(raw), parsedExpectation);
    }
  });
});

describe("synthPipeline", () => {
  it("can synthesize pipelines", () => {
    for (const [rawExpectation, parsed] of pairs) {
      assert.deepStrictEqual(synthPipeline(parsed), rawExpectation);
    }
  });
});
