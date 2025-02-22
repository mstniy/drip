import { Db, ObjectId, Timestamp } from "mongodb";
import { CSUpsertEvent, CSSubtractionEvent, CSEvent } from "./cs_event";
import { Rule } from "../rule";
import z from "zod";
import { CEACursor } from "./cea_cursor";
import { addCS, subtractCS } from "./cs_algebra";

export async function* dripCEAStart(
  db: Db,
  collectionName: string,
  syncStart: Date,
  rule: Rule,
  ruleScopedToBefore: Rule
): AsyncGenerator<CSEvent, void, void> {
  const coll = db.collection(`_drip_pcs_${collectionName}`);

  const minCTWall = z
    .array(
      z.object({
        w: z.custom<Date>((x) => x instanceof Date),
      })
    )
    .parse(
      await coll
        .find({})
        .sort({ ct: 1 })
        .project({ _id: 0, w: 1 })
        .limit(1)
        .toArray()
    )[0]?.w;

  if (typeof minCTWall === "undefined") {
    // No persisted change events yet
    return;
  }

  if (minCTWall >= syncStart) {
    throw new SyncStartTooOldError();
  }

  const minRelevantCT = z
    .array(
      z.object({
        ct: z.custom<Timestamp>((x) => x instanceof Timestamp),
        w: z.date(),
      })
    )
    .parse(
      await coll
        .find({
          w: { $gte: syncStart },
        })
        .project({ _id: 0, ct: 1, w: 1 })
        .limit(1)
        .toArray()
    )[0];

  if (typeof minRelevantCT === "undefined") {
    // No change events matching the syncStart filter
    return;
  }

  yield* dripCEAResume(
    db,
    {
      collectionName,
      clusterTime: minRelevantCT.ct,
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
  const coll = db.collection(`_drip_pcs_${cursor.collectionName}`);

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
          collectionName: cursor.collectionName,
          clusterTime: cse.ct,
          id: cse._id,
        },
      } satisfies CSUpsertEvent;
    } else {
      yield {
        operationType: "subtraction",
        cursor: {
          collectionName: cursor.collectionName,
          clusterTime: cse.ct,
          id: cse._id,
        },
        id: cse.id,
      } satisfies CSSubtractionEvent;
    }
  }
}

export class CEACannotResumeError extends Error {}

export class SyncStartTooOldError extends CEACannotResumeError {}
