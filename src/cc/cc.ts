import { ClusterTime, Document, MongoClient, ReadConcernLevel } from "mongodb";
import { CCCursor } from "./cc_cursor";
import { DripPipeline, DripProcessingPipeline } from "../drip_pipeline";
import { strict as assert } from "assert";

async function* cc_common(
  client: MongoClient,
  dbName: string,
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
      // a time window as well as to individual cursors,
      // but we want clients to be able to perform cc
      // even for large collections and resume it
      // across sync interruptions.
      // See https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/#std-label-read-concern-snapshot
      { readConcern: ReadConcernLevel.majority, session, raw }
    );

    try {
      let yielded = false;
      while (await c.hasNext()) {
        const docs = c.readBufferedDocuments();
        assert(session.clusterTime, "Expected cluster time");
        yielded = true;
        yield [session.clusterTime, docs];
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
  collectionName: string,
  cursorClusterTime: [CCCursor, ClusterTime] | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<[ClusterTime, Document[]], void, void> {
  return cc_common(
    client,
    dbName,
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
  collectionName: string,
  cursorClusterTime: [CCCursor, ClusterTime] | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<[ClusterTime, Buffer[]], void, void> {
  const gen = cc_common(
    client,
    dbName,
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
