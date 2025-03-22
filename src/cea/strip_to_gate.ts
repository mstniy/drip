import { DripPipelineStageParsed } from "./parse_ppl/drip_pipeline_stage_parsed";

export function stripToGate(
  pipeline: readonly DripPipelineStageParsed[]
): DripPipelineStageParsed[] {
  const lastMatchIndex = pipeline.map((s) => s.type).lastIndexOf("match");
  return pipeline.slice(0, lastMatchIndex + 1);
}
