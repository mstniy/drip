import { Db, ObjectId, Timestamp, UUID } from "mongodb";
import { CSUpsertEvent, CSSubtractionEvent, CSEvent } from "./cs_event";
import { Rule } from "../rule";
import z from "zod";
import { CEACursor } from "./cea_cursor";
import { addCS, subtractCS } from "./cs_algebra";

export async function* dripCEAStart(
  db: Db,
  _ns: String,
  syncStart: Date,
  rule: Rule,
  ruleScopedToBefore: Rule
): AsyncGenerator<CSEvent, void, void> {
  const coll = db.collection("manual_pcs" /* `_drip_cs_${ns}` */);
  const collectionUUID = new UUID();

  const minCT = z
    .array(z.object({ ct: z.custom<Timestamp>((x) => x instanceof Timestamp) }))
    .parse(
      await coll
        .find({
          w: { $gte: syncStart },
        })
        .project({ _id: 0, ct: 1 })
        .limit(1)
        .toArray()
    )[0]?.ct;

  if (typeof minCT === "undefined") {
    // No change events matching the syncStart filter
    return;
  }

  yield* dripCEAResume(
    db,
    {
      collectionUUID,
      clusterTime: minCT,
      id: undefined,
    },
    rule,
    ruleScopedToBefore
  );
}

export async function* dripCEAResume(
  db: Db,
  cursor: CEACursor,
  rule: Rule,
  ruleScopedToBefore: Rule // TODO: Derive this automatically
): AsyncGenerator<CSEvent, void, void> {
  const coll = db.collection("manual_pcs" /* `_drip_cs_${ns}` */);

  const maxCT = z
    .array(
      z.object({
        maxCT: z.custom<Timestamp>((x) => x instanceof Timestamp),
      })
    )
    .parse(
      await coll
        .aggregate([
          {
            $group: {
              _id: null,
              maxCT: {
                $max: "$ct",
              },
            },
          },
        ])
        .toArray()
    )[0]?.maxCT;

  if (typeof maxCT === "undefined") {
    // No PCS entries yet
    return;
  }

  const matchRelevantEvents = {
    $match: {
      ct: {
        // Further change events with this same CT might still be added,
        // so ignore any having that CT for now.
        $lt: maxCT,
      },
      $or:
        typeof cursor.id === "undefined"
          ? [{ ct: { $gte: cursor.clusterTime } }]
          : [
              { ct: { $gt: cursor.clusterTime } },
              {
                ct: { $eq: cursor.clusterTime },
                _id: { $gt: cursor.id },
              },
            ],
    },
  };

  const c1 = coll
    .aggregate([
      { $match: { o: { $in: ["i", "u"] } } },
      matchRelevantEvents,
      ...rule.stages,
      {
        $sort: {
          ct: 1,
          _id: 1,
        },
      },
      {
        $project: {
          ct: 1,
          a: 1,
        },
      },
    ])
    .map((x) =>
      z
        .object({
          _id: z.custom<ObjectId>((x) => x instanceof ObjectId),
          ct: z.custom<Timestamp>((x) => x instanceof Timestamp),
          a: z.record(z.string(), z.any()),
        })
        .parse(x)
    )
    .map((x) => {
      return {
        op: "u" as const,
        ...x,
      };
    });

  const c2a = coll
    .aggregate([
      { $match: { o: "u" } },
      matchRelevantEvents,
      ...rule.stages,
      ...ruleScopedToBefore.stages,
      {
        $sort: {
          ct: 1,
          _id: 1,
        },
      },
      {
        $project: {
          ct: 1,
        },
      },
    ])
    .map((x) =>
      z
        .object({
          _id: z.custom<ObjectId>((x) => x instanceof ObjectId),
          ct: z.custom<Timestamp>((x) => x instanceof Timestamp),
        })
        .parse(x)
    );

  const c2b = coll
    .aggregate([
      { $match: { o: "u" } },
      matchRelevantEvents,
      ...ruleScopedToBefore.stages,
      {
        $sort: {
          ct: 1,
          _id: 1,
        },
      },
      {
        $project: {
          ct: 1,
          id: "$b._id",
        },
      },
    ])
    .map((x) =>
      z
        .object({
          _id: z.custom<ObjectId>((x) => x instanceof ObjectId),
          ct: z.custom<Timestamp>((x) => x instanceof Timestamp),
          id: z.unknown(),
        })
        .parse(x)
    )
    .map((x) => {
      return { op: "s" as const, ...x };
    });

  const c3 = coll
    .aggregate([
      { $match: { o: "d" } },
      matchRelevantEvents,
      ...ruleScopedToBefore.stages,
      {
        $sort: {
          ct: 1,
          _id: 1,
        },
      },
      {
        $project: {
          ct: 1,
          id: "$b._id",
        },
      },
    ])
    .map((x) =>
      z
        .object({
          _id: z.custom<ObjectId>((x) => x instanceof ObjectId),
          ct: z.custom<Timestamp>((x) => x instanceof Timestamp),
          id: z.unknown(),
        })
        .parse(x)
    )
    .map((x) => {
      return {
        ...x,
        op: "s" as const,
      };
    });

  for await (const cse of addCS(
    // upserts due to document insertion or update
    c1[Symbol.asyncIterator](),
    addCS(
      // subtractions due to document updates
      subtractCS(c2b[Symbol.asyncIterator](), c2a[Symbol.asyncIterator]()),
      // subtractions due to document deletions
      c3[Symbol.asyncIterator]()
    )
  )) {
    if (cse.op === "u") {
      yield {
        operationType: "upsert",
        fullDocument: cse.a,
        cursor: {
          collectionUUID: cursor.collectionUUID,
          clusterTime: cse.ct,
          id: cse._id,
        },
      } satisfies CSUpsertEvent;
    } else {
      yield {
        operationType: "subtraction",
        cursor: {
          collectionUUID: cursor.collectionUUID,
          clusterTime: cse.ct,
          id: cse._id,
        },
        id: cse.id,
      } satisfies CSSubtractionEvent;
    }
  }
}
