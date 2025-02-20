import { UUID } from "mongodb";

export type CCCursor = {
  collectionUUID: UUID;
  // Must not be undefined
  // Refers to the id of the document(s) being synced.
  id: unknown;
};
