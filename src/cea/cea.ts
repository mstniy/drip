import { Db, ObjectId, ReadConcernLevel, Timestamp } from "mongodb";
import {
  CSSubtractionEvent,
  CSEvent,
  CSUpdateEvent,
  CSAdditionEvent,
  CSNoopEvent,
} from "./cs_event";
import { DripPipeline } from "../drip_pipeline";
import z from "zod";
import { CEACursor } from "./cea_cursor";
import { streamAdd, streamSquashMerge } from "./stream_algebra";
import { PCSEventCommon } from "./pcs_event";
import { oidLT } from "./oid_less";
import { minOID } from "./min_oid";
import { derivePCSCollName } from "./derive_pcs_coll_name";
import { scopeStages } from "./scope_ppl/scope_stage";
import { invertPipeline } from "./invert_ppl";
import { parseStage } from "./parse_ppl/parse_stage";
import { synthStage } from "./parse_ppl/synth_stage";

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
  pipeline: Readonly<DripPipeline>
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
    pipeline
  );
}

export async function* dripCEAResume(
  db: Db,
  cursor: CEACursor,
  pipeline: Readonly<DripPipeline>
): AsyncGenerator<CSEvent, void, void> {
  const pipelineScopedToAfter = scopeStages(pipeline, "a");
  const pipelineScopedToBefore = scopeStages(pipeline, "b");
  const pipelineScopedToBeforeInverted = invertPipeline(
    pipelineScopedToBefore.map(parseStage)
  )?.map(synthStage);
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

  if (!cursor.clusterTime.gt(minCT)) {
    throw new CEACursorNotFoundError();
  }

  const matchRelevantEvents = {
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
  };

  const c1 = coll
    .aggregate(
      [
        { $match: { ...matchRelevantEvents, o: "i" } },
        ...pipelineScopedToAfter,
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
      return {
        op: "a" as const,
        ...x,
      };
    });

  const c2a = coll
    .aggregate(
      [
        { $match: { ...matchRelevantEvents, o: "u" } },
        // Take a backup before running the given stages,
        // as they might modify the id field.
        { $addFields: { id: "$b._id" } },
        ...pipelineScopedToAfter,
        ...pipelineScopedToBefore,
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
            id: 1,
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
          id: z.unknown(),
        })
        .parse(x)
    )
    .map((x) => {
      return { op: "u" as const, ...x };
    });

  const c2b = coll
    .aggregate(
      [
        { $match: { ...matchRelevantEvents, o: "u" } },
        ...pipelineScopedToAfter,
        ...(pipelineScopedToBeforeInverted
          ? pipelineScopedToBeforeInverted
          : []),
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
        { $match: { ...matchRelevantEvents, o: "u" } },
        { $addFields: { id: "$b._id" } },
        ...pipelineScopedToBefore,
        {
          $sort: {
            ct: 1,
            _id: 1,
          },
        },
        {
          $project: {
            ct: 1,
            id: 1,
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
      return { op: "s" as const, ...x };
    });

  const c3 = coll
    .aggregate(
      [
        { $match: { ...matchRelevantEvents, o: "d" } },
        { $addFields: { id: "$b._id" } },
        ...pipelineScopedToBefore,
        {
          $sort: {
            ct: 1,
            _id: 1,
          },
        },
        {
          $project: {
            ct: 1,
            id: 1,
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

  let lastEventCT: Timestamp | undefined;

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
    lastEventCT = cse.ct;
    if (cse.op === "u") {
      yield {
        operationType: "update",
        updateDescription: cse.u,
        cursor: {
          collectionName: cursor.collectionName,
          clusterTime: cse.ct,
          id: cse._id,
        },
        id: cse.id,
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

  yield* coll
    .find(
      {
        $and: [
          matchRelevantEvents,
          {
            o: "n",
            ...(lastEventCT
              ? {
                  ct: {
                    $gt: lastEventCT,
                  },
                }
              : {}),
          },
        ],
      },
      { readConcern: ReadConcernLevel.majority }
    )
    .sort({ ct: -1 })
    .limit(1)
    .project({ ct: 1 })
    .map((x) =>
      z
        .object({
          _id: z.instanceof(ObjectId),
          ct: z.instanceof(Timestamp),
        })
        .parse(x)
    )
    .map((x) => {
      return {
        operationType: "noop" as const,
        cursor: {
          collectionName: cursor.collectionName,
          clusterTime: x.ct,
          id: x._id,
        },
      } satisfies CSNoopEvent;
    });
}

export class CEACannotResumeError extends Error {}

export class CEACursorNotFoundError extends CEACannotResumeError {}
