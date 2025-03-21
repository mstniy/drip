export type DripPipelineStage =
  | { $addFields: Record<string, unknown> }
  // We exclude the $fill stage because it is
  // poorly documented
  //| PipelineStage.Fill
  | { $match: Record<string, unknown> }
  | { $project: Record<string, unknown> }
  | { $replaceRoot: { newRoot: unknown } }
  | { $replaceWith: unknown }
  | { $set: Record<string, unknown> }
  | { $unset: string[] | string };

export type DripPipeline = DripPipelineStage[];
