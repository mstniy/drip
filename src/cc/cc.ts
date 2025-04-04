import {
  Abortable,
  AggregateOptions,
  AggregationCursor,
  Db,
  Document,
  ReadConcernLevel,
  Timestamp,
} from "mongodb";
import { CCCursor } from "./cc_cursor";
import { DripPipeline, DripProcessingPipeline } from "../drip_pipeline";
import z from "zod";

function makeAggregation(
  db: Db,
  collectionName: string,
  cursor: CCCursor | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline: Readonly<DripPipeline> | undefined,
  options?: AggregateOptions & Abortable
): AggregationCursor<Document> {
  const c = db
    .collection(collectionName)
    .aggregate(
      [
        ...(cursor ? [{ $match: { _id: { $gt: cursor.id } } }] : []),
        ...pipeline,
        { $sort: { _id: 1 } },
        ...(processingPipeline ?? []),
      ],
      { ...options, readConcern: ReadConcernLevel.majority }
    );

  return c;
}

async function getClusterTime(db: Db) {
  const ct = z.object({ ct: z.instanceof(Timestamp) }).parse(
    (
      await db
        .aggregate([{ $documents: [{ ct: "$$CLUSTER_TIME" }] }], {
          readConcern: ReadConcernLevel.majority,
        })
        .toArray()
    )[0]
  ).ct;

  return ct;
}

async function* cc(
  db: Db,
  collectionName: string,
  cursor: CCCursor | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<Document, Timestamp, void> {
  const c = makeAggregation(
    db,
    collectionName,
    cursor,
    pipeline,
    processingPipeline
  );

  yield* c;

  // Note that the majority read concern level has
  // monotonic read guarantees, so the cluster time
  // we get here is a valid upper bound for
  // ending the CEA stage.
  return getClusterTime(db);
}

export async function dripCCStart(
  db: Db,
  collectionName: string,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): Promise<{
  ccStart: Timestamp;
  gen: AsyncGenerator<Document, Timestamp, void>;
}> {
  const ccStart = await getClusterTime(db);
  // A similar argument to that in [cc] follows here.
  return {
    ccStart,
    gen: cc(db, collectionName, undefined, pipeline, processingPipeline),
  };
}

export function dripCCResume(
  db: Db,
  collectionName: string,
  cursor: CCCursor,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<Document, Timestamp, void> {
  return cc(db, collectionName, cursor, pipeline, processingPipeline);
}

async function* cc_raw(
  db: Db,
  collectionName: string,
  cursor: CCCursor | undefined,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<Buffer, Timestamp, void> {
  const c = makeAggregation(
    db,
    collectionName,
    cursor,
    pipeline,
    processingPipeline,
    { raw: true }
  );

  for await (const buffer_ of c) {
    const unsafe = buffer_ as unknown as Buffer;
    // See https://mongodb.github.io/node-mongodb-native/6.13/interfaces/AggregateOptions.html#raw
    const safe = Buffer.alloc(unsafe.byteLength);
    safe.set(unsafe, 0);
    yield safe;
  }

  // See [cc].
  return getClusterTime(db);
}

export async function dripCCRawStart(
  db: Db,
  collectionName: string,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): Promise<{
  ccStart: Timestamp;
  gen: AsyncGenerator<Buffer, Timestamp, void>;
}> {
  const ccStart = await getClusterTime(db);
  // A similar argument to that in [cc] follows here.
  return {
    ccStart,
    gen: cc_raw(db, collectionName, undefined, pipeline, processingPipeline),
  };
}

export function dripCCRawResume(
  db: Db,
  collectionName: string,
  cursor: CCCursor,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<Buffer, Timestamp, void> {
  return cc_raw(db, collectionName, cursor, pipeline, processingPipeline);
}
