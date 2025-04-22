import { Timestamp } from "mongodb";
import { CEACursor } from "./cea_cursor";

export interface CSEventCommon {
  cursor: CEACursor;
  clusterTime: Timestamp;
}

export interface CSAdditionEvent extends CSEventCommon {
  operationType: "addition";
  fullDocument: Record<string, unknown>;
}

export interface CSUpdateEvent extends CSEventCommon {
  operationType: "update";
  updateDescription: Record<string, unknown>;
  id: unknown;
}

export interface CSReplaceEvent extends CSEventCommon {
  operationType: "replace";
  fullDocument: Record<string, unknown>;
}

export interface CSSubtractionEvent extends CSEventCommon {
  operationType: "subtraction";
  id: unknown;
}

export interface CSNoopEvent extends CSEventCommon {
  operationType: "noop";
}

export type CSEvent =
  | CSAdditionEvent
  | CSUpdateEvent
  | CSReplaceEvent
  | CSSubtractionEvent
  | CSNoopEvent;
