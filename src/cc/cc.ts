import {
  Abortable,
  AggregateOptions,
  AggregationCursor,
  Db,
  Document,
} from "mongodb";
import { CCCursor } from "./cc_cursor";
import { Rule } from "../rule";

function makeAggregation(
  db: Db,
  nsOrCursor: string | CCCursor,
  rule: Rule,
  options?: AggregateOptions & Abortable
): AggregationCursor<Document> {
  const collectionName =
    typeof nsOrCursor === "string" ? nsOrCursor : nsOrCursor.collectionName;

  const c = db
    .collection(collectionName)
    .aggregate(
      [
        ...(typeof nsOrCursor === "string"
          ? []
          : [{ $match: { _id: { $gt: nsOrCursor.id } } }]),
        ...rule.stages,
        { $sort: { _id: 1 } },
      ],
      options
    );

  return c;
}

export async function* dripCC(
  db: Db,
  nsOrCursor: string | CCCursor,
  rule: Rule
): AsyncGenerator<Document, void, void> {
  const c = makeAggregation(db, nsOrCursor, rule);

  yield* c;
}

export async function* dripCCRaw(
  db: Db,
  nsOrCursor: string | CCCursor,
  rule: Rule
): AsyncGenerator<Buffer, void, void> {
  const c = makeAggregation(db, nsOrCursor, rule, { raw: true });

  for await (const buffer_ of c) {
    const unsafe = buffer_ as any as Buffer;
    // See https://mongodb.github.io/node-mongodb-native/6.13/interfaces/AggregateOptions.html#raw
    const safe = Buffer.alloc(unsafe.byteLength);
    safe.set(unsafe, 0);
    yield safe;
  }
}
