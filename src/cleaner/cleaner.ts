import { Db, ObjectId, ReadConcernLevel, Timestamp } from "mongodb";
import { derivePCSCollName } from "../cea/derive_pcs_coll_name";
import z from "zod";

export async function expirePCSEvents(
  collectionName: string,
  metadataDb: Db,
  until: Date
): Promise<void> {
  const pcsColl = metadataDb.collection(derivePCSCollName(collectionName));

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

  // Cannot use deleteMany - we must guarantee that
  // we never delete an event with a higher
  // [cluster time, id] followed by a lower one.
  const idZodSchema = z.object({ _id: z.instanceof(ObjectId) });
  const expiredEvents = pcsColl
    .find(
      { ct: { $lte: lastExpiredCT } },
      { readConcern: ReadConcernLevel.majority }
    )
    .sort({ ct: 1, _id: 1 })
    .project({ _id: 1 })
    .map((o) => idZodSchema.parse(o));

  try {
    while (await expiredEvents.hasNext()) {
      const ids = expiredEvents.readBufferedDocuments();
      // Right away schedule the next batch
      void expiredEvents.hasNext();

      const bulkDelete = ids.map((id) => {
        return {
          deleteOne: {
            filter: { _id: id._id },
          },
        };
      });

      await pcsColl.bulkWrite(bulkDelete, {
        ordered: true, // important - see above
        writeConcern: { w: ReadConcernLevel.majority },
      });
    }
  } finally {
    await expiredEvents.close();
  }
}
