import { PipelineStage } from "mongoose";

export type RulePipelineStage =
  | PipelineStage.AddFields
  // We exclude the $fill stage because it is
  // poorly documented
  //| PipelineStage.Fill
  | PipelineStage.Match
  | PipelineStage.Project
  | PipelineStage.Redact
  | PipelineStage.ReplaceRoot
  | PipelineStage.ReplaceWith
  | PipelineStage.Set
  | PipelineStage.Unset;

export interface Rule {
  stages: readonly RulePipelineStage[];
}
