import { Db } from "mongodb";
import { derivePCSCollName } from "../cea/derive_pcs_coll_name";
import { changeEventToPCSEvent } from "./change_event_to_pcs_event";

export async function startPersister(
  db: Db,
  collectionName: string
): Promise<void> {
  const pcsColl = db.collection(derivePCSCollName(collectionName));
  const cs = db.collection(collectionName).watch(
    [
      {
        $match: {
          operationType: { $in: ["insert", "update", "delete"] },
        },
      },
    ],
    // TODO: Consult the metadata table to resume from an existing resume token, if one exists and is still valid.
    // What to do if it is not valid anymore? Cannot keep writing to the PCS collection, that would be misleading.
    // Maybe throw an exception?
    // To get field disambiguation
    { showExpandedEvents: true }
  );

  for await (const ce of cs) {
    const pcse = changeEventToPCSEvent(ce);
    if (typeof pcse !== "undefined") {
      // TODO: Be more clever: save in batches
      // TODO: Persist the resume token. Not just here, but listen for RESUME_TOKEN_CHANGED events.
      await pcsColl.insertOne(pcse);
    }
  }
}
