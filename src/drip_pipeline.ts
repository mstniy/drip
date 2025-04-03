export type DripPipelineProcessingStage =
  | { $addFields: Record<string, unknown> }
  // We exclude the $fill stage because it is
  // poorly documented
  //| PipelineStage.Fill
  | { $project: Record<string, unknown> }
  | { $replaceRoot: { newRoot: unknown } }
  | { $replaceWith: unknown }
  | { $set: Record<string, unknown> }
  | { $unset: string[] | string };

export type DripPipelineStage =
  | DripPipelineProcessingStage
  | { $match: Record<string, unknown> };

export type DripPipeline = DripPipelineStage[];
export type DripProcessingPipeline = DripPipelineProcessingStage[];
