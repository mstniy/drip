import { ObjectId, Timestamp } from "mongodb";
import z from "zod";

const zodPCSEventCommon = z.object({
  _id: z.custom<ObjectId>((x) => x instanceof ObjectId),
  // cluster time
  ct: z.custom<Timestamp>((x) => x instanceof Timestamp),
  // wall clock
  w: z.date(),
});

export const zodPCSInsertionEvent = zodPCSEventCommon.and(
  z.object({
    // operation type
    o: z.literal("i"),
    // key document
    k: z.record(z.string(), z.any()),
    // after document
    a: z.record(z.string(), z.any()),
  })
);

export const zodPCSUpdateEvent = zodPCSEventCommon.and(
  z.object({
    o: z.literal("u"),
    // key document
    k: z.record(z.string(), z.any()),
    // before document
    b: z.record(z.string(), z.any()),
    a: z.record(z.string(), z.any()),
    // update description
    u: z.record(z.string(), z.any()),
  })
);

export const zodPCSDeletionEvent = zodPCSEventCommon.and(
  z.object({
    o: z.literal("d"),
    // key document
    k: z.record(z.string(), z.any()),
    b: z.record(z.string(), z.any()),
  })
);

export const zodPCSNoopEvent = zodPCSEventCommon.and(
  z.object({
    o: z.literal("n"),
  })
);

export type PCSEventCommon = z.infer<typeof zodPCSEventCommon>;
export type PCSInsertionEvent = z.infer<typeof zodPCSInsertionEvent>;
export type PCSUpdateEvent = z.infer<typeof zodPCSUpdateEvent>;
export type PCSDeletionEvent = z.infer<typeof zodPCSDeletionEvent>;
export type PCSNoopEvent = z.infer<typeof zodPCSNoopEvent>;
