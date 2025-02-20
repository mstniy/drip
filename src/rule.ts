import { PipelineStage } from "mongoose";

type RulePipelineStage =
  | PipelineStage.AddFields
  | PipelineStage.Fill
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
