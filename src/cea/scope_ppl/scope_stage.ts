import _ from "lodash";
import { RulePipelineStage } from "../../rule";
import { InvalidStage } from "./invalid_stage";
import { isObjectExpression, scopeExpression } from "./scope_expression";
import { PipelineStage } from "mongoose";
import { scopeQueryClause } from "./scope_query";

function scopeStageValueKeys(
  stageValue: Record<string, unknown>,
  root: string
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(stageValue).map(([k, v]) => [
      `${root}.${k}`,
      scopeExpression(v, root, {}),
    ])
  );
}

export function scopeStage(
  s: RulePipelineStage,
  root: string
): RulePipelineStage {
  const keys = Object.keys(s);
  if (keys.length !== 1) {
    throw new InvalidStage(
      "A pipeline stage specification object must contain exactly one field."
    );
  }
  const stage = keys[0]!;
  const stageValue = Object.values(s)[0] as string;
  switch (stage) {
    case "$redact":
    case "$replaceRoot":
    case "$replaceWith":
      return _.mapValues(s, (v) =>
        scopeExpression(v, root, {})
      ) as RulePipelineStage;
    case "$addFields":
    case "$project":
    case "$set":
      return _.mapValues(s, (v) =>
        scopeStageValueKeys(v, root)
      ) as RulePipelineStage;
    case "$match":
      if (!isObjectExpression(stageValue)) {
        throw new InvalidStage(
          "the match filter must be an expression in an object"
        );
      }
      return { $match: scopeQueryClause(stageValue, root) };
    case "$unset": {
      const unsets = stageValue as PipelineStage.Unset["$unset"];
      return {
        $unset:
          typeof unsets === "string"
            ? `${root}.${unsets}`
            : unsets.map((p) => `${root}.${p}`),
      };
    }
    default:
      throw new InvalidStage(`Unrecognized pipeline stage name: '${stage}'`);
  }
}

export function scopeStages(
  s: readonly RulePipelineStage[],
  root: string
): RulePipelineStage[] {
  return s.map((ss) => scopeStage(ss, root));
}
