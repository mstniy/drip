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
import { DripPipeline } from "../drip_pipeline";
import z from "zod";
import { streamMap } from "../cea/stream_algebra";

function makeAggregation(
  db: Db,
  collNameOrCursor: string | CCCursor,
  pipeline: Readonly<DripPipeline>,
  options?: AggregateOptions & Abortable
): AggregationCursor<Document> {
  const collectionName =
    typeof collNameOrCursor === "string"
      ? collNameOrCursor
      : collNameOrCursor.collectionName;

  const c = db
    .collection(collectionName)
    .aggregate(
      [
        ...(typeof collNameOrCursor === "string"
          ? []
          : [{ $match: { _id: { $gt: collNameOrCursor.id } } }]),
        ...pipeline,
        { $sort: { _id: 1 } },
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
  collNameOrCursor: string | CCCursor,
  pipeline: Readonly<DripPipeline>
): AsyncGenerator<Document, Timestamp, void> {
  const c = makeAggregation(db, collNameOrCursor, pipeline);

  yield* c;

  return getClusterTime(db);
}

export async function* dripCCRaw(
  db: Db,
  collNameOrCursor: string | CCCursor,
  pipeline: Readonly<DripPipeline>
): AsyncGenerator<Buffer, Timestamp, void> {
  const c = makeAggregation(db, collNameOrCursor, pipeline, { raw: true });

  yield* streamMap(c[Symbol.asyncIterator](), (buffer_) => {
    const unsafe = buffer_ as unknown as Buffer;
    // See https://mongodb.github.io/node-mongodb-native/6.13/interfaces/AggregateOptions.html#raw
    const safe = Buffer.alloc(unsafe.byteLength);
    safe.set(unsafe, 0);
    return safe;
  });

  return getClusterTime(db);
}
