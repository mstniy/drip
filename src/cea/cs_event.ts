import { Document } from "mongodb";
import { CEACursor } from "./cea_cursor";

export interface CSEventCommon {
  cursor: CEACursor;
}

export interface CSUpsertEvent extends CSEventCommon {
  operationType: "upsert";
  fullDocument: Document;
}

export interface CSSubtractionEvent extends CSEventCommon {
  operationType: "subtraction";
  id: unknown;
}

export type CSEvent = CSUpsertEvent | CSSubtractionEvent;
