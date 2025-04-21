import { ChangeStreamDocument, ObjectId, Timestamp } from "mongodb";
import {
  PCSDeletionEvent,
  PCSEventCommon,
  PCSInsertionEvent,
  PCSUpdateEvent,
} from "../cea/pcs_event";
import z from "zod";
import { updateDescriptionToU as updateDescriptionToU } from "./update_description_to_u";
import { strict as assert } from "assert";

export function changeEventToPCSEvent(
  ce: ChangeStreamDocument
): PCSEventCommon | undefined {
  if (ce.operationType === "insert") {
    const res = {
      _id: new ObjectId(),
      o: "i",
      ct: z.instanceof(Timestamp).parse(ce.clusterTime),
      k: z.record(z.unknown()).parse(ce.documentKey),
      a: z.record(z.unknown()).parse(ce.fullDocument),
    } satisfies PCSInsertionEvent;
    return res;
  } else if (ce.operationType === "update" || ce.operationType === "replace") {
    assert(typeof ce.fullDocumentBeforeChange !== "undefined");
    const res = {
      _id: new ObjectId(),
      o: "u",
      ct: z.instanceof(Timestamp).parse(ce.clusterTime),
      k: z.record(z.unknown()).parse(ce.documentKey),
      b: z.record(z.unknown()).parse(ce.fullDocumentBeforeChange),
      a: z.record(z.unknown()).parse(ce.fullDocument),
      ...(ce.operationType === "update"
        ? { u: updateDescriptionToU(ce.updateDescription) }
        : {}),
    } satisfies PCSUpdateEvent;
    return res;
  } else if (ce.operationType === "delete") {
    const res = {
      _id: new ObjectId(),
      o: "d",
      ct: z.instanceof(Timestamp).parse(ce.clusterTime),
      k: z.record(z.unknown()).parse(ce.documentKey),
      b: z.record(z.unknown()).parse(ce.fullDocumentBeforeChange),
    } satisfies PCSDeletionEvent;
    return res;
  }

  // Unknown operation
  return undefined;
}
