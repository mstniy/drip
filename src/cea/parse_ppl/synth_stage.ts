import { DripPipelineStage } from "../../drip_pipeline";
import { DripPipelineStageParsed } from "./drip_pipeline_stage_parsed";

export function synthStage(s: DripPipelineStageParsed): DripPipelineStage {
  if (s.type === "addFields") {
    return { $addFields: s.fields };
  }
  if (s.type === "match") {
    return { $match: s.filter };
  }
  if (s.type === "project") {
    return { $project: s.fields };
  }
  if (s.type === "redact") {
    return { $redact: s.expr };
  }
  if (s.type === "replaceRoot") {
    return { $replaceRoot: { newRoot: s.newRoot } };
  }
  if (s.type === "replaceWith") {
    return { $replaceWith: s.expr };
  }
  if (s.type === "set") {
    return { $set: s.fields };
  }
  s.type satisfies "unset";
  return { $unset: s.fields };
}
