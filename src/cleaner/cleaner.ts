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

  const maxCT = z
    .object({ ct: z.instanceof(Timestamp) })
    .optional()
    .parse(
      (
        await pcsColl
          .find({}, { readConcern: ReadConcernLevel.majority })
          .sort({ ct: -1 })
          .project({ _id: 0, ct: 1 })
          .limit(1)
          .toArray()
      )[0]
    )?.ct;

  if (!maxCT) {
    // No persisted events
    return;
  }

  const lastExpiredCT_ = z
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

  if (!lastExpiredCT_) {
    // No expired events
    return;
  }

  // Avoid cleaning the tail of the PCS
  const lastExpiredCT = lastExpiredCT_.gte(maxCT)
    ? new Timestamp(maxCT.subtract(1))
    : lastExpiredCT_;

  // Use a transaction to guarantee that
  // we never delete an event with a higher
  // [cluster time, id] followed by a lower one.
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
