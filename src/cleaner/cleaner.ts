import { MongoClient, ReadConcernLevel, Timestamp } from "mongodb";
import { derivePCSCollName } from "../cea/derive_pcs_coll_name";
import z from "zod";

export async function expirePCSEvents(
  collectionName: string,
  client: MongoClient,
  metadataDbName: string,
  until: Date
): Promise<void> {
  const pcsColl = client
    .db(metadataDbName)
    .collection(derivePCSCollName(collectionName));

  const lastExpiredCT = z
    .object({ ct: z.instanceof(Timestamp) })
    .optional()
    .parse(
      (
        await pcsColl
          .find(
            { w: { $lt: until } },
            { readConcern: ReadConcernLevel.majority }
          )
          .sort({ w: -1 })
          .project({ _id: 0, ct: 1 })
          .limit(1)
          .toArray()
      )[0]
    )?.ct;

  if (!lastExpiredCT) {
    // No expired events
    return;
  }

  // Use a transaction to guarantee that
  // we never delete an event with a higher
  // [cluster time, id] followed by a lower one.
  // Note that we also assume the perister will not
  // later persist a change event with ct <= lastExpiredCT.
  await client.withSession((session) =>
    session.withTransaction(
      async (session) => {
        await pcsColl.deleteMany({ ct: { $lte: lastExpiredCT } }, { session });
      },
      {
        readConcern: ReadConcernLevel.majority,
        writeConcern: { w: "majority" },
      }
    )
  );
}
