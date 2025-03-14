import { Collection, Db, ObjectId, ReadConcernLevel, Timestamp } from "mongodb";
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
import z from "zod";

export async function startPersister(
  watchCollection: Collection,
  metadataDb: Db
): Promise<void> {
  const collectionName = watchCollection.collectionName;
  const promiseTrain = new PromiseTrain();
  const pcsColl = metadataDb.collection(derivePCSCollName(collectionName));
  const metadataColl = metadataDb.collection<DripMetadata>(
    MetadataCollectionName
  );

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
      .map((o) => zodDripMetadata.pick({ resumeToken: true }).parse(o))
      .toArray()
  )[0]?.resumeToken;

  const cs = watchCollection.watch(
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

  cs.on("resumeTokenChanged", (async (resumeToken) => {
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
    const newResumeTokenData = z
      .string()
      .parse((resumeToken as Record<string, unknown>)["_data"]);
    if (
      newResumeTokenData !==
      ((lastResumeToken as Record<string, unknown> | undefined) ?? {})["_data"]
    ) {
      const decoded = decodeResumeToken(newResumeTokenData);
      await pushPCSEventUpdateMetadata(
        {
          v: 1,
          _id: new ObjectId(),
          // mongodb-resumetoken-decoder and the actual driver use
          // incompatible bson versions, so translate between
          // the two
          ct: Timestamp.fromBits(decoded.timestamp.low, decoded.timestamp.high),
          o: "n",
          // The resume token does not have a wall clock,
          // so our best bet is to attach ours
          w: new Date(),
        } satisfies PCSNoopEvent,
        resumeToken
      );
    }
  }) as (rt: unknown) => void);

  while (true) {
    const ce = await cs.next();
    lastResumeToken = ce._id;
    const pcse = changeEventToPCSEvent(ce);
    if (typeof pcse !== "undefined") {
      await pushPCSEventUpdateMetadata(pcse, ce._id);
    }
  }
}
