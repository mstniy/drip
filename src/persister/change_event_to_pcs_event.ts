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
      v: 1,
      ct: z.instanceof(Timestamp).parse(ce.clusterTime),
      w: z.date().parse((ce as any).wallTime),
      k: z.record(z.string(), z.any()).parse(ce.documentKey),
      a: z.record(z.string(), z.any()).parse(ce.fullDocument),
    } satisfies PCSInsertionEvent;
    return res;
  } else if (ce.operationType === "update") {
    assert(typeof ce.fullDocumentBeforeChange !== "undefined");
    const res = {
      _id: new ObjectId(),
      o: "u",
      v: 1,
      ct: z.instanceof(Timestamp).parse(ce.clusterTime),
      w: z.date().parse((ce as any).wallTime),
      k: z.record(z.string(), z.any()).parse(ce.documentKey),
      b: z.record(z.string(), z.any()).parse(ce.fullDocumentBeforeChange),
      a: z.record(z.string(), z.any()).parse(ce.fullDocument),
      u: updateDescriptionToU(ce.updateDescription),
    } satisfies PCSUpdateEvent;
    return res;
  } else if (ce.operationType === "delete") {
    const res = {
      _id: new ObjectId(),
      o: "d",
      v: 1,
      ct: z.instanceof(Timestamp).parse(ce.clusterTime),
      w: z.date().parse((ce as any).wallTime),
      k: z.record(z.string(), z.any()).parse(ce.documentKey),
      b: z.record(z.string(), z.any()).parse(ce.fullDocumentBeforeChange),
    } satisfies PCSDeletionEvent;
    return res;
  }

  // Unknown operation
  return undefined;
}
