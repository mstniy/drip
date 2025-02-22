import { ObjectId, Timestamp } from "mongodb";

export type CEACursor = {
  collectionName: string;
  clusterTime: Timestamp;
  // If set to undefined: the change stream returns all change events
  // with cluster time >= the given one.
  // Refers to the id of the persisted change event(s) being synced.
  id: ObjectId | undefined;
};
