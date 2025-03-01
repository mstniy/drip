import { ObjectId, Timestamp } from "mongodb";
import z from "zod";

const zodPCSEventCommon = z.object({
  _id: z.instanceof(ObjectId),
  // cluster time
  ct: z.instanceof(Timestamp),
  // peristed change stream version
  v: z.literal(1),
  o: z.string(),
});

export const zodPCSInsertionEvent = zodPCSEventCommon.merge(
  z.object({
    // operation type
    o: z.literal("i"),
    // key document
    k: z.record(z.string(), z.any()),
    // after document
    a: z.record(z.string(), z.any()),
    // wall clock
    w: z.date(),
  })
);

export const zodPCSUpdateEvent = zodPCSEventCommon.merge(
  z.object({
    o: z.literal("u"),
    // key document
    k: z.record(z.string(), z.any()),
    // before document
    b: z.record(z.string(), z.any()),
    a: z.record(z.string(), z.any()),
    // update description
    u: z.record(z.string(), z.any()),
    // wall clock
    w: z.date(),
  })
);

export const zodPCSDeletionEvent = zodPCSEventCommon.merge(
  z.object({
    o: z.literal("d"),
    // key document
    k: z.record(z.string(), z.any()),
    b: z.record(z.string(), z.any()),
    // wall clock
    w: z.date(),
  })
);

export const zodPCSNoopEvent = zodPCSEventCommon.merge(
  z.object({
    o: z.literal("n"),
  })
);

export type PCSEventCommon = z.infer<typeof zodPCSEventCommon>;
export type PCSInsertionEvent = z.infer<typeof zodPCSInsertionEvent>;
export type PCSUpdateEvent = z.infer<typeof zodPCSUpdateEvent>;
export type PCSDeletionEvent = z.infer<typeof zodPCSDeletionEvent>;
export type PCSNoopEvent = z.infer<typeof zodPCSNoopEvent>;
