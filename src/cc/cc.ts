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
  const ct = z
    .object({ ct: z.instanceof(Timestamp) })
    .parse(
      (
        await db
          .aggregate([{ $documents: [{ ct: "$$CLUSTER_TIME" }] }])
          .toArray()
      )[0]
    ).ct;

  return ct;
}

export async function* dripCC(
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

  return getClusterTime(db);
}

export async function* dripCCRaw(
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

  return getClusterTime(db);
}
