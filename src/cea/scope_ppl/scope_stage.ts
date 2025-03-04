import _ from "lodash";
import { RulePipelineStage } from "../../rule";
import { InvalidStage } from "./invalid_stage";
import { scopeExpression } from "./scope_expression";
import { PipelineStage } from "mongoose";

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
  const stageValue = Object.values(s)[0];
  switch (stage) {
    case "$redact":
    case "$replaceRoot":
    case "$replaceWith":
      return _.mapValues(s, (v) => scopeExpression(v, root, {})) as any;
    case "$addFields":
    case "$project":
    case "$set":
      return {
        [stage]: scopeStageValueKeys(stageValue, root),
      } as any;
    case "$match": // TODO: match is kinda more complicated, as there have a whole different syntax
      throw "not implemented yet :(";
    case "$unset":
      const unsets = stageValue as PipelineStage.Unset["$unset"];
      return {
        $unset:
          typeof unsets === "string"
            ? `${root}.${unsets}`
            : unsets.map((p) => `${root}.${p}`),
      };
    default:
      throw new InvalidStage(`Unrecognized pipeline stage name: '${stage}'`);
  }
}
