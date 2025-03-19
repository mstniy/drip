import { PipelineStage } from "mongoose";

export type DripPipelineStage =
  | PipelineStage.AddFields
  // We exclude the $fill stage because it is
  // poorly documented
  //| PipelineStage.Fill
  | PipelineStage.Match
  | PipelineStage.Project
  | PipelineStage.Redact
  | PipelineStage.ReplaceRoot
  | { $replaceWith: unknown }
  | PipelineStage.Set
  | PipelineStage.Unset;

export type DripPipeline = DripPipelineStage[];
