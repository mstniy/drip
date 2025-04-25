import { DripPipelineStageParsed } from "./parse_ppl/drip_pipeline_stage_parsed";

function invertStage(stage: DripPipelineStageParsed): DripPipelineStageParsed {
  if (stage.type === "match") {
    return {
      type: "match",
      filter: { $nor: [stage.filter] },
    };
  }

  return stage;
}
export function invertPipeline(
  pipeline: Readonly<DripPipelineStageParsed[]>
): DripPipelineStageParsed[][] {
  const numMatches = pipeline.filter((s) => s.type === "match").length;

  if (numMatches === 0) {
    return [];
  }

  if (numMatches === 1) {
    return [pipeline.map(invertStage)];
  }

  // Inverting a pipeline with multiple $match stages requires
  // it to be split up into multiple pipelines, which we don't
  // bother doing, so we return a single sub-pipeline that
  // matches all.
  return [[]];
}
