import {
  Collection,
  MongoClient,
  ObjectId,
  ReadConcernLevel,
  ResumeToken,
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
import { FlushBuffer } from "./flush_buffer";
import { noopyCS } from "./noopy_change_stream";

// The defaults here are pretty solid,
// so you should not change them unless
// you really know what you are doing.
export interface PersisterOptions {
  maxAwaitTimeMS?: number;
  minNoopIntervalMS?: number;
}

export async function* runPersister(
  metadataClient: MongoClient,
  metadataDbName: string,
  watchCollection: Collection,
  options?: PersisterOptions
): AsyncGenerator<void, void, void> {
  const collectionName = watchCollection.collectionName;
  const promiseTrain = new PromiseTrain();
  const metadataDb = metadataClient.db(metadataDbName);
  const pcsColl = metadataDb.collection(derivePCSCollName(collectionName));
  const metadataColl = metadataDb.collection<DripMetadata>(
    MetadataCollectionName
  );

  async function pushPCSEventsUpdateMetadata(
    events: [ResumeToken, PCSEventCommon][]
  ) {
    if (events.length > 0) {
      await promiseTrain.push(() =>
        metadataClient.withSession((session) =>
          session.withTransaction(
            async (session) => {
              await pcsColl.insertMany(
                events.map((e) => e[1]),
                {
                  ordered: true,
                  session,
                }
              );
              await metadataColl.findOneAndUpdate(
                { _id: collectionName },
                { $set: { resumeToken: events[events.length - 1]![0] } },
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

  const flushBuffer = new FlushBuffer<[ResumeToken, PCSEventCommon]>(
    MAX_BUFFER_LENGTH,
    (events) => pushPCSEventsUpdateMetadata(events)
  );

  const minNoopIntervalMS =
    typeof options?.minNoopIntervalMS === "number"
      ? options.minNoopIntervalMS
      : // The default MongoDB noop interval is 10 seconds,
        // so the default here should also be less than 10 seconds.
        // It should not be too small, though, as then we cannot
        // avoid the CT loop described below.
        8000;

  const cs = watchCollection.watch(
    [
      {
        $match: {
          operationType: { $in: ["insert", "update", "replace", "delete"] },
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
      ...(typeof options?.maxAwaitTimeMS === "number"
        ? { maxAwaitTimeMS: options.maxAwaitTimeMS }
        : {}),
    }
  );

  const ncs = noopyCS(cs);

  try {
    let lastEventClock: Date | undefined;
    for await (const event of ncs) {
      // This lets the caller stop us gracefully
      yield;
      if (event.type === "nothing") {
        // Nothing
      } else if (event.type === "noop") {
        const w = new Date();
        if (
          !lastEventClock ||
          // Avoid persisting every single change in the cluster time,
          // as this leads to an infinite loop of us persisting the CT,
          // and the CT advancing because of the new write, if we are
          // persisting to the same cluster we are reading the CS from.
          w.getTime() - lastEventClock.getTime() >= minNoopIntervalMS ||
          // Means the wall clock went backwards - so be prudent
          // and persist the noop
          lastEventClock > w
        ) {
          const noop = {
            _id: new ObjectId(),
            ct: event.ct,
            o: "n",
            w: w,
          } satisfies PCSNoopEvent;
          lastEventClock = w;
          await flushBuffer.push([event.rt, noop]);
        }
      } else {
        event.type satisfies "change";

        const pcse = changeEventToPCSEvent(event.change);
        if (typeof pcse !== "undefined") {
          lastEventClock = new Date();
          await flushBuffer.push([event.change._id, pcse]);
        }
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
