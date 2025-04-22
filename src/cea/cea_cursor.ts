import { ObjectId } from "mongodb";

export type CEACursor = {
  // Refers to the id of the persisted change event(s) being synced.
  id: ObjectId;
};
