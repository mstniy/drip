import {
  Abortable,
  AggregateOptions,
  AggregationCursor,
  Db,
  Document,
  ReadConcernLevel,
} from "mongodb";
import { CCCursor } from "./cc_cursor";
import { Rule } from "../rule";

function makeAggregation(
  db: Db,
  collNameOrCursor: string | CCCursor,
  rule: Rule,
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
        ...rule.stages,
        { $sort: { _id: 1 } },
      ],
      { ...options, readConcern: ReadConcernLevel.majority }
    );

  return c;
}

export async function* dripCC(
  db: Db,
  collNameOrCursor: string | CCCursor,
  rule: Rule
): AsyncGenerator<Document, void, void> {
  const c = makeAggregation(db, collNameOrCursor, rule);

  yield* c;
}

export async function* dripCCRaw(
  db: Db,
  collNameOrCursor: string | CCCursor,
  rule: Rule
): AsyncGenerator<Buffer, void, void> {
  const c = makeAggregation(db, collNameOrCursor, rule, { raw: true });

  for await (const buffer_ of c) {
    const unsafe = buffer_ as any as Buffer;
    // See https://mongodb.github.io/node-mongodb-native/6.13/interfaces/AggregateOptions.html#raw
    const safe = Buffer.alloc(unsafe.byteLength);
    safe.set(unsafe, 0);
    yield safe;
  }
}
