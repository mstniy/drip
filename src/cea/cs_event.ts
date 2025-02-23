import { Document } from "mongodb";
import { CEACursor } from "./cea_cursor";

export interface CSEventCommon {
  cursor: CEACursor;
}

export interface CSAdditionEvent extends CSEventCommon {
  operationType: "addition";
  fullDocument: Document;
}

export interface CSUpdateEvent extends CSEventCommon {
  operationType: "update";
  updateDescription: Document;
}

export interface CSSubtractionEvent extends CSEventCommon {
  operationType: "subtraction";
  id: unknown;
}

export type CSEvent = CSAdditionEvent | CSUpdateEvent | CSSubtractionEvent;
