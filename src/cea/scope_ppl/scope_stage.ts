import { scopeExpression } from "./scope_expression";
import { scopeQueryClause } from "./scope_query";
import { DripPipelineStageParsed } from "../parse_ppl/drip_pipeline_stage_parsed";

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
  s: DripPipelineStageParsed,
  root: string
): DripPipelineStageParsed {
  if (s.type === "addFields") {
    return { type: "addFields", fields: scopeStageValueKeys(s.fields, root) };
  }
  if (s.type === "project") {
    return { type: "project", fields: scopeStageValueKeys(s.fields, root) };
  }
  if (s.type === "set") {
    return { type: "set", fields: scopeStageValueKeys(s.fields, root) };
  }
  if (s.type === "replaceRoot") {
    return {
      type: "set",
      fields: { [root]: scopeExpression(s.newRoot, root, {}) },
    };
  }
  if (s.type === "replaceWith") {
    return {
      type: "set",
      fields: { [root]: scopeExpression(s.expr, root, {}) },
    };
  }
  if (s.type === "match") {
    return { type: "match", filter: scopeQueryClause(s.filter, root) };
  }
  s.type satisfies "unset";

  const unsets = s.fields;
  return {
    type: "unset",
    fields:
      typeof unsets === "string"
        ? `${root}.${unsets}`
        : unsets.map((p) => `${root}.${p}`),
  };
}

export function scopeStages(
  s: readonly DripPipelineStageParsed[],
  root: string
): DripPipelineStageParsed[] {
  return s.map((ss) => scopeStage(ss, root));
}
