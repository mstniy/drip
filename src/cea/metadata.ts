import z from "zod";

export const MetadataCollectionName = "_drip_metadata_v1";

export const zodDripMetadata = z.object({
  // The name of the collection
  _id: z.string(),
  resumeToken: z.unknown(),
});

export type DripMetadata = z.infer<typeof zodDripMetadata>;
