export type CCCursor = {
  collectionName: string;
  // Must not be undefined
  // Refers to the id of the document(s) being synced.
  id: unknown;
};
