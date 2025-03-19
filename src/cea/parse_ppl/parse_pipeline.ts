import { PipelineStage } from "mongoose";

import { DripPipelineStage } from "../../drip_pipeline";
import { InvalidStage } from "./invalid_stage";
import { DripPipelineStageParsed } from "./drip_pipeline_stage_parsed";

function parseStage(s: DripPipelineStage): DripPipelineStageParsed {
  const keys = Object.keys(s);
  if (keys.length !== 1) {
    throw new InvalidStage(
      "A pipeline stage specification object must contain exactly one field."
    );
  }
  if ("$redact" in s) {
    return { type: "redact", expr: s.$redact };
  }
  if ("$replaceRoot" in s) {
    return {
      type: "replaceRoot",
      newRoot: s.$replaceRoot.newRoot,
    };
  }
  if ("$replaceWith" in s) {
    return {
      type: "replaceWith",
      expr: s.$replaceWith,
    };
  }
  if ("$addFields" in s) {
    return {
      type: "addFields",
      fields: s.$addFields,
    };
  }
  if ("$project" in s) {
    return {
      type: "project",
      fields: s.$project,
    };
  }
  if ("$set" in s) {
    return {
      type: "set",
      fields: s.$set,
    };
  }
  if ("$match" in s) {
    return { type: "match", filter: s.$match };
  }

  s satisfies PipelineStage.Unset;
  return { type: "unset", fields: s.$unset };
}

export function parsePipeline(
  pipeline: readonly DripPipelineStage[]
): DripPipelineStageParsed[] {
  return pipeline.map(parseStage);
}
