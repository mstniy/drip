import {
  Abortable,
  AggregateOptions,
  Db,
  Document,
  ReadConcernLevel,
  Timestamp,
} from "mongodb";
import { CCCursor } from "./cc_cursor";
import { DripPipeline, DripProcessingPipeline } from "../drip_pipeline";
import z from "zod";

async function* makeAggregation(
  db: Db,
  collNameOrCursor: string | CCCursor,
  pipeline: Readonly<DripPipeline>,
  processingPipeline: Readonly<DripPipeline> | undefined,
  options?: AggregateOptions & Abortable
): AsyncGenerator<Document[], void, void> {
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
        ...(processingPipeline ?? []),
      ],
      { ...options, readConcern: ReadConcernLevel.majority }
    );

  try {
    let hasNext = c.hasNext();
    while (await hasNext) {
      const docs = c.readBufferedDocuments();
      // Right away schedule the next batch
      hasNext = c.hasNext();
      yield docs;
    }
  } finally {
    await c.close();
  }
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
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<Document, Timestamp, void> {
  const c = makeAggregation(db, collNameOrCursor, pipeline, processingPipeline);

  for await (const batch of c) {
    yield* batch;
  }

  return getClusterTime(db);
}

export async function* dripCCRaw(
  db: Db,
  collNameOrCursor: string | CCCursor,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>
): AsyncGenerator<Buffer, Timestamp, void> {
  const c = makeAggregation(
    db,
    collNameOrCursor,
    pipeline,
    processingPipeline,
    { raw: true }
  )[Symbol.asyncIterator]();

  for await (const batch of c) {
    yield* batch.map((b) => b as unknown as Buffer);
  }

  return getClusterTime(db);
}
