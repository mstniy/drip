import { ObjectId, Timestamp } from "mongodb";
import z from "zod";

export const zodCEACursor = z
  .object({
    collectionName: z.string(),
    clusterTime: z.custom<Timestamp>((x) => x instanceof Timestamp),
    id: z.custom<ObjectId>((x) => x instanceof ObjectId).optional(),
  })
  .strict();
