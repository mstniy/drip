import {
  DripPipelineStageParsed,
  Match,
} from "./parse_ppl/drip_pipeline_stage_parsed";

function invertStage(stage: DripPipelineStageParsed): DripPipelineStageParsed {
  if (stage.type === "match") {
    return {
      type: "match",
      filter: { $nor: [stage.filter] },
    };
  }

  return stage;
}

export function combineAdjacentMatches(
  pipeline: Readonly<DripPipelineStageParsed[]>
): DripPipelineStageParsed[] {
  const res: DripPipelineStageParsed[] = [];

  let i = 0;
  while (i < pipeline.length) {
    let nextNonMatch = i;
    while (
      nextNonMatch < pipeline.length &&
      pipeline[nextNonMatch]!.type === "match"
    ) {
      nextNonMatch++;
    }
    if (nextNonMatch === i) {
      // Push nothing
    } else if (nextNonMatch === i + 1) {
      // Push the only match stage
      res.push(pipeline[i]!);
    } else {
      // Combine together the adjacent match stages and push
      res.push({
        type: "match",
        filter: {
          $and: pipeline.slice(i, nextNonMatch).map((m) => (m as Match).filter),
        },
      });
    }
    if (nextNonMatch < pipeline.length) {
      res.push(pipeline[nextNonMatch]!);
    }
    i = nextNonMatch + 1;
  }

  return res;
}

export function invertPipeline(
  pipeline_: Readonly<DripPipelineStageParsed[]>
): DripPipelineStageParsed[][] {
  const pipeline = combineAdjacentMatches(pipeline_);
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
