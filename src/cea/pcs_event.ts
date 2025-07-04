import { ObjectId, Timestamp } from "mongodb";
import z from "zod";

const zodPCSEventCommon = z.object({
  _id: z.instanceof(ObjectId),
  // cluster time
  ct: z.instanceof(Timestamp),
  o: z.string(),
});

export const zodPCSInsertionEvent = zodPCSEventCommon.merge(
  z.object({
    // operation type
    o: z.literal("i"),
    // key document
    k: z.record(z.string(), z.unknown()),
    // after document
    a: z.record(z.string(), z.unknown()),
  })
);

export const zodPCSUpdateEvent = zodPCSEventCommon.merge(
  z.object({
    o: z.literal("u"),
    // key document
    k: z.record(z.string(), z.unknown()),
    // before document
    b: z.record(z.string(), z.unknown()),
    a: z.record(z.string(), z.unknown()),
    // update description
    // absent for updates coming from replace events
    u: z.record(z.string(), z.unknown()).optional(),
  })
);

export const zodPCSDeletionEvent = zodPCSEventCommon.merge(
  z.object({
    o: z.literal("d"),
    // key document
    k: z.record(z.string(), z.unknown()),
    b: z.record(z.string(), z.unknown()),
  })
);

export const zodPCSNoopEvent = zodPCSEventCommon.merge(
  z.object({
    o: z.literal("n"),
    // wall clock
    w: z.date(),
  })
);

export const zodPCSEvent = z.discriminatedUnion("o", [
  zodPCSDeletionEvent,
  zodPCSNoopEvent,
  zodPCSInsertionEvent,
  zodPCSUpdateEvent,
]);

export type PCSInsertionEvent = z.infer<typeof zodPCSInsertionEvent>;
export type PCSUpdateEvent = z.infer<typeof zodPCSUpdateEvent>;
export type PCSDeletionEvent = z.infer<typeof zodPCSDeletionEvent>;
export type PCSNoopEvent = z.infer<typeof zodPCSNoopEvent>;
export type PCSEvent = z.infer<typeof zodPCSEvent>;
