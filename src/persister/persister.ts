import { Collection, MongoClient, ObjectId, ReadConcernLevel } from "mongodb";
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

export interface PersisterOptions {
  maxAwaitTimeMS?: number;
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
    for await (const event of ncs) {
      if (event.type === "nothing") {
        // This lets the caller stop us gracefully
        yield;
      } else if (event.type === "noop") {
        const noop = {
          _id: new ObjectId(),
          ct: event.ct,
          o: "n",
          w: new Date(),
        } satisfies PCSNoopEvent;
        await flushBuffer.push(noop);
      } else {
        event.type satisfies "change";

        const pcse = changeEventToPCSEvent(event.change);
        if (typeof pcse !== "undefined") {
          lastResumeToken = event.change._id;
          await flushBuffer.push(pcse);
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
