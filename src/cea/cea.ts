import { Db, ObjectId, ReadConcernLevel, Timestamp } from "mongodb";
import {
  CSSubtractionEvent,
  CSEvent,
  CSUpdateEvent,
  CSAdditionEvent,
} from "./cs_event";
import { Rule } from "../rule";
import z from "zod";
import { CEACursor } from "./cea_cursor";
import { streamAdd, streamSquashMerge } from "./stream_algebra";
import { PCSEventCommon } from "./pcs_event";
import { oidLT } from "./oid_less";
import { minOID } from "./min_oid";
import { derivePCSCollName } from "./derive_pcs_coll_name";

function pcseLT(
  a: Pick<PCSEventCommon, "ct" | "_id">,
  b: Pick<PCSEventCommon, "ct" | "_id">
) {
  return a.ct.lt(b.ct) || (a.ct.eq(b.ct) && oidLT(a._id, b._id));
}

export async function* dripCEAStart(
  db: Db,
  collectionName: string,
  syncStart: Date,
  rule: Rule,
  ruleScopedToBefore: Rule
): AsyncGenerator<CSEvent, void, void> {
  const coll = db.collection(derivePCSCollName(collectionName));

  const minRelevantCT = z
    .array(
      z.object({
        ct: z.instanceof(Timestamp),
        w: z.date(),
      })
    )
    .parse(
      await coll
        .find(
          {
            w: { $gte: syncStart },
            v: 1,
          },
          { readConcern: ReadConcernLevel.majority }
        )
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
      id: minOID,
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
  const coll = db.collection(derivePCSCollName(cursor.collectionName));

  const minCT = z
    .array(
      z.object({
        ct: z.instanceof(Timestamp),
      })
    )
    .parse(
      await coll
        .find({ v: 1 }, { readConcern: ReadConcernLevel.majority })
        .sort({ ct: 1 })
        .limit(1)
        .project({ _id: 0, ct: 1 })
        .toArray()
    )[0]?.ct;

  const maxCT = z
    .array(
      z.object({
        ct: z.instanceof(Timestamp),
      })
    )
    .parse(
      await coll
        .find({ v: 1 }, { readConcern: ReadConcernLevel.majority })
        .sort({ ct: -1 })
        .limit(1)
        .project({ _id: 0, ct: 1 })
        .toArray()
    )[0]?.ct;

  if (typeof minCT === "undefined" || typeof maxCT === "undefined") {
    // No PCS entries yet
    return;
  }

  if (cursor.clusterTime.compare(minCT) !== 1) {
    throw new CEACursorNotFoundError();
  }

  const matchRelevantEvents = {
    $match: {
      v: 1,
      ct: {
        // Further change events with this same CT might still be added,
        // so ignore any having that CT for now.
        $lt: maxCT,
      },
      $or: [
        { ct: { $gt: cursor.clusterTime } },
        {
          ct: { $eq: cursor.clusterTime },
          _id: { $gt: cursor.id },
        },
      ],
    },
  };

  const c1 = coll
    .aggregate(
      [
        { $match: { v: 1, o: "i" } },
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
      ],
      { readConcern: ReadConcernLevel.majority }
    )
    .map((x) =>
      z
        .object({
          _id: z.instanceof(ObjectId),
          ct: z.instanceof(Timestamp),
          a: z.record(z.string(), z.any()),
        })
        .parse(x)
    )
    .map((x) => {
      return {
        op: "a" as const,
        ...x,
      };
    });

  const c2a = coll
    .aggregate(
      [
        { $match: { v: 1, o: "u" } },
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
            u: 1,
          },
        },
      ],
      { readConcern: ReadConcernLevel.majority }
    )
    .map((x) =>
      z
        .object({
          _id: z.instanceof(ObjectId),
          ct: z.instanceof(Timestamp),
          u: z.record(z.string(), z.unknown()),
        })
        .parse(x)
    )
    .map((x) => {
      return { op: "u" as const, ...x };
    });

  const c2b = coll
    .aggregate(
      [
        { $match: { v: 1, o: "u" } },
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
      ],
      { readConcern: ReadConcernLevel.majority }
    )
    .map((x) =>
      z
        .object({
          _id: z.instanceof(ObjectId),
          ct: z.instanceof(Timestamp),
          a: z.record(z.string(), z.unknown()),
        })
        .parse(x)
    )
    .map((x) => {
      return { op: "a" as const, ...x };
    });

  const c2c = coll
    .aggregate(
      [
        { $match: { v: 1, o: "u" } },
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
      ],
      { readConcern: ReadConcernLevel.majority }
    )
    .map((x) =>
      z
        .object({
          _id: z.instanceof(ObjectId),
          ct: z.instanceof(Timestamp),
          id: z.string(),
        })
        .parse(x)
    )
    .map((x) => {
      return { op: "s" as const, ...x };
    });

  const c3 = coll
    .aggregate(
      [
        { $match: { v: 1, o: "d" } },
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
      ],
      { readConcern: ReadConcernLevel.majority }
    )
    .map((x) =>
      z
        .object({
          _id: z.instanceof(ObjectId),
          ct: z.instanceof(Timestamp),
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

  for await (const cse of streamAdd(
    // additions due to document insertions
    c1[Symbol.asyncIterator](),
    streamAdd(
      streamSquashMerge(
        [
          // updates
          c2a[Symbol.asyncIterator](),
          // additions due to document updates (when cleaned of updates)
          c2b[Symbol.asyncIterator](),
          // subtractions due to document updates (when cleaned of updates)
          c2c[Symbol.asyncIterator](),
        ],
        pcseLT
      ),
      // subtractions due to document deletions
      c3[Symbol.asyncIterator](),
      pcseLT
    ),
    pcseLT
  )) {
    if (cse.op === "u") {
      yield {
        operationType: "update",
        updateDescription: cse.u,
        cursor: {
          collectionName: cursor.collectionName,
          clusterTime: cse.ct,
          id: cse._id,
        },
      } satisfies CSUpdateEvent;
    } else if (cse.op === "a") {
      yield {
        operationType: "addition",
        fullDocument: cse.a,
        cursor: {
          collectionName: cursor.collectionName,
          clusterTime: cse.ct,
          id: cse._id,
        },
      } satisfies CSAdditionEvent;
    } else {
      cse.op satisfies "s";
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

export class CEACursorNotFoundError extends CEACannotResumeError {}
