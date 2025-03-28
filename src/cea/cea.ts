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
import { streamAppend, streamSquashMerge, streamTake } from "./stream_algebra";
import { PCSEventCommon } from "./pcs_event";
import { oidLT } from "./oid_less";
import { minOID } from "./min_oid";
import { derivePCSCollName } from "./derive_pcs_coll_name";
import { scopeStages } from "./scope_ppl/scope_stage";
import { invertPipeline } from "./invert_ppl";
import { parsePipeline } from "./parse_ppl/parse_pipeline";
import { synthPipeline, synthStage } from "./parse_ppl/synth_pipeline";
import { stripToGate } from "./strip_to_gate";

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
            o: "n",
            w: { $gte: syncStart },
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
  const pipelineParsed = parsePipeline(pipeline);
  const pipelineScopedToAfter = synthPipeline(scopeStages(pipelineParsed, "a"));
  const pipelineScopedToBefore = scopeStages(stripToGate(pipelineParsed), "b");
  const pipelineScopedToBeforeSynthed = synthPipeline(pipelineScopedToBefore);
  const pipelineScopedToBeforeInverted = invertPipeline(
    pipelineScopedToBefore
  ).map((ppl) => ppl.map(synthStage));
  const coll = db.collection(derivePCSCollName(cursor.collectionName));

  const minCT = z
    .array(
      z.object({
        ct: z.instanceof(Timestamp),
      })
    )
    .parse(
      await coll
        .find({}, { readConcern: ReadConcernLevel.majority })
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
        .find({}, { readConcern: ReadConcernLevel.majority })
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

  if (!cursor.clusterTime.lt(maxCT)) {
    return;
  }

  // As we use the tuple [clusterTime, id] as the sort order,
  // we need two queries.
  // Or one query with an $or clause, but Mongo has trouble
  // planning them optimally.
  const matchRelevantEvents = [
    {
      ct: {
        $eq: cursor.clusterTime,
      },
      _id: { $gt: cursor.id },
    },
    {
      ct: {
        $lt: maxCT,
        $gt: cursor.clusterTime,
      },
    },
  ];

  const c1 = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "i" } },
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
        })
        [Symbol.asyncIterator]()
    )
  );

  const c2a = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "u" } },
            // Take a backup before running the given stages,
            // as they might modify the id field.
            { $addFields: { id: "$b._id" } },
            ...pipelineScopedToAfter,
            ...pipelineScopedToBeforeSynthed,
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
        })
        [Symbol.asyncIterator]()
    )
  );

  const c2bs = pipelineScopedToBeforeInverted.map((ppl) =>
    streamAppend(
      matchRelevantEvents.map((mre) =>
        coll
          .aggregate(
            [
              { $match: { ...mre, o: "u" } },
              ...pipelineScopedToAfter,
              ...ppl,
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
          })
          [Symbol.asyncIterator]()
      )
    )
  );

  const c2c = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "u" } },
            { $addFields: { id: "$b._id" } },
            ...pipelineScopedToBeforeSynthed,
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
        })
        [Symbol.asyncIterator]()
    )
  );

  const c3 = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "d" } },
            { $addFields: { id: "$b._id" } },
            ...pipelineScopedToBeforeSynthed,
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
        })
        [Symbol.asyncIterator]()
    )
  );

  const cs = streamSquashMerge(
    [
      // additions due to document insertions
      c1,
      // subtractions due to document deletions
      c3,
      // updates
      c2a,
      // additions due to document updates (might need to be cleaned of updates,
      // we do not invert all pipelines)
      ...c2bs,
      // subtractions due to document updates (when cleaned of updates)
      c2c,
    ],
    pcseLT
  );

  let lastEventCT: Timestamp | undefined;

  for await (const cse of cs) {
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

  yield* streamTake(
    1,
    streamAppend(
      matchRelevantEvents.toReversed().map((mre) =>
        coll
          .find(
            {
              $and: [
                mre,
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
          })
          [Symbol.asyncIterator]()
      )
    )
  );
}

export class CEACannotResumeError extends Error {}

export class CEACursorNotFoundError extends CEACannotResumeError {}
