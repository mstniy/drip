import { ObjectId, Timestamp } from "mongodb";

export type CEACursor = {
  clusterTime: Timestamp;
  // Refers to the id of the persisted change event(s) being synced.
  id: ObjectId;
};
