import {
  Collection,
  MongoClient,
  ObjectId,
  ReadConcernLevel,
  Timestamp,
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
import z from "zod";
import { FlushBuffer } from "./flush_buffer";

export async function* runPersister(
  metadataClient: MongoClient,
  metadataDbName: string,
  watchCollection: Collection
): AsyncGenerator<void, void, void> {
  const collectionName = watchCollection.collectionName;
  const promiseTrain = new PromiseTrain();
  const metadataDb = metadataClient.db(metadataDbName);
  const pcsColl = metadataDb.collection(derivePCSCollName(collectionName));
  const metadataColl = metadataDb.collection<DripMetadata>(
    MetadataCollectionName
  );

  async function pushPCSEventsUpdateMetadata(
    events: PCSEventCommon[],
    resumeToken: unknown
  ) {
    await promiseTrain.push(() =>
      metadataClient.withSession((session) =>
        session.withTransaction(
          async (session) => {
            await pcsColl.insertMany(events, {
              ordered: true,
              session,
            });
            await metadataColl.findOneAndUpdate(
              { _id: collectionName },
              { $set: { resumeToken: resumeToken } },
              {
                upsert: true,
                session,
              }
            );
          },
          {
            readConcern: ReadConcernLevel.majority,
            writeConcern: { w: "majority" },
          }
        )
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

  const MAX_BUFFER_LENGTH = 1000;

  let lastResumeToken: unknown;
  const flushBuffer = new FlushBuffer<PCSEventCommon>(
    MAX_BUFFER_LENGTH,
    (events) => pushPCSEventsUpdateMetadata(events, lastResumeToken)
  );

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

  try {
    cs.on("resumeTokenChanged", (async (resumeToken) => {
      // The Mongo node driver has a flaw for change stream where
      // it calls resumeTokenChanged before actually reporting
      // the change event to the user. Thus, naively
      // relying on resumeTokenChanged to keep track of the
      // resume token is racy. Hence, we delay persisting the
      // received resume token by two microtasks.
      // Two microtasks are enough because:
      // - The loop on the change stream has one microtask
      // delay between it receiving the change from next()
      // and passing it along to the flush buffer.
      // - The implementation of next() also does not have
      // any microtask delays between it emitting the
      // resumeTokenChanged event and returning the change.
      // See https://github.com/mongodb/node-mongodb-native/blob/44bc5a880230a5be93afc9e2a4fa0a4586481edd/src/change_stream.ts#L746
      await Promise.resolve();
      await Promise.resolve();
      const newResumeTokenData = z
        .string()
        .parse((resumeToken as Record<string, unknown>)["_data"]);
      if (
        newResumeTokenData !==
        ((lastResumeToken as Record<string, unknown> | undefined) ?? {})[
          "_data"
        ]
      ) {
        lastResumeToken = resumeToken;
        const decoded = decodeResumeToken(newResumeTokenData);
        const event = {
          _id: new ObjectId(),
          // mongodb-resumetoken-decoder and the actual driver use
          // incompatible bson versions, so translate between
          // the two
          ct: Timestamp.fromBits(decoded.timestamp.low, decoded.timestamp.high),
          o: "n",
          w: new Date(),
        } satisfies PCSNoopEvent;
        // Awaiting this would be meaningless
        // as we are inside an EventEmitter
        // callback
        void flushBuffer.push(event);
      }
    }) as (rt: unknown) => void);

    while (true) {
      yield;
      const ce = await cs.next();
      const pcse = changeEventToPCSEvent(ce);
      if (typeof pcse !== "undefined") {
        lastResumeToken = ce._id;
        await flushBuffer.push(pcse);
      }
    }
  } finally {
    // Cancel the timer from FlushBuffer
    flushBuffer.abort();
    // Wait for the existing operations on the
    // PromiseTrain
    await promiseTrain.push(() => Promise.resolve());
    await cs.close();
  }
}
