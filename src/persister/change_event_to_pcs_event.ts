import { ChangeStreamDocument, ObjectId, Timestamp } from "mongodb";
import {
  PCSDeletionEvent,
  PCSEvent,
  PCSInsertionEvent,
  PCSUpdateEvent,
} from "../cea/pcs_event";
import z from "zod";
import { updateDescriptionToU as updateDescriptionToU } from "./update_description_to_u";

const zodInsertSchema = z.object({
  clusterTime: z.instanceof(Timestamp),
  documentKey: z.record(z.string(), z.unknown()),
  fullDocument: z.record(z.string(), z.unknown()),
});

const zodUpdateSchema = z.object({
  clusterTime: z.instanceof(Timestamp),
  documentKey: z.record(z.string(), z.unknown()),
  fullDocument: z.record(z.string(), z.unknown()),
  fullDocumentBeforeChange: z.record(z.string(), z.unknown()),
});

const zodDeleteSchema = z.object({
  clusterTime: z.instanceof(Timestamp),
  documentKey: z.record(z.string(), z.unknown()),
  fullDocumentBeforeChange: z.record(z.string(), z.unknown()),
});

export function changeEventToPCSEvent(
  ce: ChangeStreamDocument
): PCSEvent | undefined {
  if (ce.operationType === "insert") {
    const ceParsed = zodInsertSchema.parse(ce);
    const res = {
      _id: new ObjectId(),
      o: "i",
      ct: ceParsed.clusterTime,
      k: ceParsed.documentKey,
      a: ceParsed.fullDocument,
    } satisfies PCSInsertionEvent;
    return res;
  } else if (ce.operationType === "update" || ce.operationType === "replace") {
    const ceParsed = zodUpdateSchema.parse(ce);
    const res = {
      _id: new ObjectId(),
      o: "u",
      ct: ceParsed.clusterTime,
      k: ceParsed.documentKey,
      b: ceParsed.fullDocumentBeforeChange,
      a: ceParsed.fullDocument,
      ...(ce.operationType === "update"
        ? { u: updateDescriptionToU(ce.updateDescription) }
        : {}),
    } satisfies PCSUpdateEvent;
    return res;
  } else if (ce.operationType === "delete") {
    const ceParsed = zodDeleteSchema.parse(ce);
    const res = {
      _id: new ObjectId(),
      o: "d",
      ct: ceParsed.clusterTime,
      k: ceParsed.documentKey,
      b: ceParsed.fullDocumentBeforeChange,
    } satisfies PCSDeletionEvent;
    return res;
  }

  // Unknown operation
  return undefined;
}
