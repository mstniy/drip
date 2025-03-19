export type AddFields = {
  type: "addFields";
  fields: Record<string, unknown>;
};
export type Match = {
  type: "match";
  filter: Record<string, unknown>;
};
export type Project = {
  type: "project";
  fields: Record<string, unknown>;
};
export type Redact = {
  type: "redact";
  expr: unknown;
};
export type ReplaceRoot = {
  type: "replaceRoot";
  newRoot: unknown;
};
export type ReplaceWith = {
  type: "replaceWith";
  expr: unknown;
};
export type Set = {
  type: "set";
  fields: Record<string, unknown>;
};
export type Unset = {
  type: "unset";
  fields: string | string[];
};

export type DripPipelineStageParsed =
  | AddFields
  | Match
  | Project
  | Redact
  | ReplaceRoot
  | ReplaceWith
  | Set
  | Unset;
