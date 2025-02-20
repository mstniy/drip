import { UUID } from "mongodb";

export type CCCursor = {
  collectionUUID: UUID;
  // Note that unlike [CEACursor::documentKey], this does NOT
  // include the shard keys.
  key: unknown;
};
