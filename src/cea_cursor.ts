import { Document, Timestamp, UUID } from "mongodb";

export type CEACursor = {
  collectionUUID: UUID;
  clusterTime: Timestamp;
  // MongoDB includes the full shard path here for sharded collections
  // (see https://www.mongodb.com/docs/manual/reference/change-events/insert/).
  // We keep these to allow the persisted change stream to be sharded
  // in a similar same way as the original collection.
  documentKey: Document;
};
