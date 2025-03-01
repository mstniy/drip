import {
  Db,
  ObjectId,
  ReadConcern,
  ReadConcernLevel,
  Timestamp,
  WriteConcern,
} from "mongodb";
import { derivePCSCollName } from "../cea/derive_pcs_coll_name";
import { changeEventToPCSEvent } from "./change_event_to_pcs_event";
import {
  DripMetadata,
  MetadataCollectionName,
  zodDripMetadata,
} from "../cea/metadata";
import { PromiseTrain } from "./promise_train";
import { PCSEventCommon, PCSNoopEvent } from "../cea/pcs_event";
import { decodeResumeToken } from "mongodb-resumetoken-decoder";

export async function startPersister(
  db: Db,
  collectionName: string
): Promise<void> {
  const promiseTrain = new PromiseTrain();
  const pcsColl = db.collection(derivePCSCollName(collectionName));
  const metadataColl = db.collection<DripMetadata>(MetadataCollectionName);

  async function pushPCSEventUpdateMetadata(
    pcse: PCSEventCommon,
    resumeToken: unknown
  ) {
    await promiseTrain.push(() =>
      pcsColl.insertOne(pcse, { writeConcern: { w: "majority" } })
    );
    await promiseTrain.push(() =>
      metadataColl.findOneAndUpdate(
        { _id: collectionName },
        { $set: { resumeToken: resumeToken } },
        {
          upsert: true,
          readConcern: ReadConcernLevel.majority,
          writeConcern: { w: "majority" },
        }
      )
    );
  }

  const persistedResumeToken = (
    await metadataColl
      .find(
        {
          _id: collectionName,
        },
        { readConcern: ReadConcernLevel.majority }
      )
      .project({ _id: 0, resumeToken: 1 })
      .map(zodDripMetadata.pick({ resumeToken: true }).parse)
      .toArray()
  )[0]?.resumeToken;

  const cs = db.collection(collectionName).watch(
    [
      {
        $match: {
          operationType: { $in: ["insert", "update", "delete"] },
        },
      },
    ],
    {
      // To get field disambiguation
      showExpandedEvents: true,
      // We also persist pre- and post-images
      fullDocument: "required",
      fullDocumentBeforeChange: "required",
      resumeAfter: persistedResumeToken,
      readConcern: ReadConcernLevel.majority,
    }
  );

  let lastResumeToken: unknown;

  cs.on("resumeTokenChanged", async (resumeToken) => {
    // The Mongo node driver has a flaw for change stream where
    // it calls resumeTokenChanged before actually reporting
    // the change event to the user. Thus, naively
    // relying on resumeTokenChanged to keep track of the
    // resume token is racy. Hence, we delay persisting the
    // received resume token by two microtasks.
    // Two microtasks are enough because:
    // - The next() loop below has one microtask
    // delay between it receiving the change from next()
    // and passing it along to insertOne()
    // - The next() call also does not have any microtask
    // delays between it emitting the resumeTokenChanged
    // event and returning the change
    // See https://github.com/mongodb/node-mongodb-native/blob/44bc5a880230a5be93afc9e2a4fa0a4586481edd/src/change_stream.ts#L746
    await Promise.resolve();
    await Promise.resolve();
    if ((resumeToken as any)._data !== (lastResumeToken as any)?._data) {
      const decoded = decodeResumeToken((resumeToken as any)._data);
      await pushPCSEventUpdateMetadata(
        {
          v: 1,
          _id: new ObjectId(),
          // mongodb-resumetoken-decoder and the actual driver use
          // incompatible bson versions, so translate between
          // the two
          ct: Timestamp.fromBits(decoded.timestamp.low, decoded.timestamp.high),
          o: "n",
        } satisfies PCSNoopEvent,
        resumeToken
      );
    }
  });

  while (true) {
    const ce = await cs.next();
    lastResumeToken = ce._id;
    const pcse = changeEventToPCSEvent(ce);
    if (typeof pcse !== "undefined") {
      await pushPCSEventUpdateMetadata(pcse, ce._id);
    }
  }
}
