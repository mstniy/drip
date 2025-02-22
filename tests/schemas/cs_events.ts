import { zodCEACursor } from "./cea_cursor";
import z from "zod";

export const zodCSEventCommon = z.object({
  cursor: zodCEACursor,
});

export const zodCSUpsertEvent = zodCSEventCommon
  .merge(
    z.object({
      operationType: z.literal("upsert"),
      fullDocument: z.record(z.string(), z.any()),
    })
  )
  .strict();

export const zodCSSubtractionEvent = zodCSEventCommon
  .merge(
    z.object({
      operationType: z.literal("subtraction"),
      id: z.unknown(),
    })
  )
  .strict();
