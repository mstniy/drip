import {
  ClusterTime,
  Db,
  Document,
  MongoClient,
  ReadConcernLevel,
} from "mongodb";
import { CCCursor } from "./cc_cursor";
import { DripPipeline, DripProcessingPipeline } from "../drip_pipeline";
import { strict as assert } from "assert";
import { derivePCSCollName } from "../cea/derive_pcs_coll_name";

async function* cc_common(
  client: MongoClient,
  dbName: string,
  metadataDb: Db,
  collectionName: string,
  cursorClusterTime: [CCCursor, ClusterTime] | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline: Readonly<DripProcessingPipeline> | undefined,
  raw: boolean
): AsyncGenerator<[ClusterTime, Document[]], void, void> {
  // We need causal consistency for monotonic reads
  const session = client.startSession({ causalConsistency: true });
  try {
    const db = client.db(dbName);

    if (cursorClusterTime) {
      // Make sure the data we get is not more stale than
      // ccStart. This makes it a valid lower bound for
      // the duration over which cc took place.
      // See https://github.com/mongodb/specifications/blob/43d2c7bacd62249de8d2173bf8ee39e6fd7a686e/source/causal-consistency/causal-consistency.md#causalconsistency
      session.advanceClusterTime(cursorClusterTime[1]);
    } else {
      // This is the very first batch the client will get.
      // Run a dummy db operation to get a lower bound
      // on the cluster time. We cannot rely on the cluster
      // time returned by the initial batch, as it might
      // have advanced while the read was going on.
      await db
        .aggregate([{ $documents: [] }], {
          readConcern: ReadConcernLevel.majority,
          session,
        })
        .toArray();

      assert(session.clusterTime, "Expected cluster time");

      // Check if there already is at least one persisted
      // change event with a lower cluster time.
      // As otherwise, doing CC is pointless as CEA
      // will fail anyway.
      const olderPCSEExists =
        (
          await metadataDb
            .collection(derivePCSCollName(collectionName))
            .find(
              { ct: { $lt: session.clusterTime.clusterTime } },
              { readConcern: ReadConcernLevel.majority, session }
            )
            .project({ _id: 0 })
            .limit(1)
            .toArray()
        ).length > 0;

      if (!olderPCSEExists) {
        throw new CCWaitForPersisterError();
      }

      yield [session.clusterTime, []];
    }

    const c = db.collection(collectionName).aggregate(
      [
        ...(cursorClusterTime
          ? [{ $match: { _id: { $gt: cursorClusterTime[0].id } } }]
          : []),
        ...pipeline,
        { $sort: { _id: 1 } },
        ...(processingPipeline ?? []),
      ],
      // Note that the majority read concern does provide
      // monotonic reads.
      // See https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/
      // Also note that the snapshot read concern is not
      // useful in our case, as it is limited to
      // a likely-too-small time window.
      // See https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/#std-label-read-concern-snapshot
      { readConcern: ReadConcernLevel.majority, session, raw }
    );

    try {
      let yielded = false;
      while (await c.hasNext()) {
        const docs = c.readBufferedDocuments();
        assert(session.clusterTime, "Expected cluster time");
        yielded = true;
        // We need not yield a signed cluster time,
        // as any cluster time after the initial one
        // (which we have already yielded above, if needed)
        // is only to be used to determine until when to
        // continue CEA before declaring the resulting
        // snapshot consistent.
        yield [{ clusterTime: session.clusterTime.clusterTime }, docs];
      }

      if (!yielded) {
        // Yield an empty batch so that the client
        // knows from which cluster time to start CEA
        assert(session.clusterTime, "Expected cluster time");
        yield [session.clusterTime, []];
      }
    } finally {
      await c.close();
    }
  } finally {
    await session.endSession();
  }
}

export function dripCC(
  client: MongoClient,
  dbName: string,
  metadataDb: Db,
  collectionName: string,
  cursorClusterTime: [CCCursor, ClusterTime] | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<[ClusterTime, Document[]], void, void> {
  return cc_common(
    client,
    dbName,
    metadataDb,
    collectionName,
    cursorClusterTime,
    pipeline,
    processingPipeline,
    false
  );
}

export async function* dripCCRaw(
  client: MongoClient,
  dbName: string,
  metadataDb: Db,
  collectionName: string,
  cursorClusterTime: [CCCursor, ClusterTime] | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<[ClusterTime, Buffer[]], void, void> {
  const gen = cc_common(
    client,
    dbName,
    metadataDb,
    collectionName,
    cursorClusterTime,
    pipeline,
    processingPipeline,
    true
  );
  for await (const r of gen) {
    // See https://mongodb.github.io/node-mongodb-native/6.13/interfaces/AggregateOptions.html#raw
    const buffersUnsafe = r[1] as unknown[] as Buffer[];
    const buffersSafe = buffersUnsafe.map((unsafe) => {
      const safe = Buffer.alloc(unsafe.byteLength);
      safe.set(unsafe, 0);
      return safe;
    });

    yield [r[0], buffersSafe];
  }
}

export class CCWaitForPersisterError extends Error {}
