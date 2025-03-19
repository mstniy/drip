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
): DripPipelineStageParsed[] | undefined {
  const numMatches = pipeline.filter((s) => s.type === "match").length;

  if (numMatches === 0) {
    return [{ type: "match", filter: { $expr: false } }];
  }

  if (numMatches === 1) {
    return pipeline
      .slice(0, pipeline.findIndex((s) => s.type === "match") + 1)
      .map(invertStage);
  }

  // Inverting a pipeline with multiple $match stages requires
  // it to be split up into multiple pipelines, which we don't
  // bother doing.
  return undefined;
}
