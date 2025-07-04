import { Db, ObjectId, ReadConcernLevel, Timestamp } from "mongodb";
import {
  CSSubtractionEvent,
  CSEvent,
  CSUpdateEvent,
  CSAdditionEvent,
  CSNoopEvent,
  CSReplaceEvent,
} from "./cs_event";
import { DripPipeline, DripProcessingPipeline } from "../drip_pipeline";
import z from "zod";
import { CEACursor } from "./cea_cursor";
import { streamAppend, streamSquashMerge, streamTake } from "./stream_algebra";
import { PCSEvent } from "./pcs_event";
import { oidLT } from "./oid_less";
import { derivePCSCollName } from "./derive_pcs_coll_name";
import { scopeStages } from "./scope_ppl/scope_stage";
import { invertPipeline } from "./invert_ppl";
import { parsePipeline } from "./parse_ppl/parse_pipeline";
import { synthPipeline, synthStage } from "./parse_ppl/synth_pipeline";
import { stripToGate } from "./strip_to_gate";
import { CEAOptions } from "./options";

function pcseLT(
  a: Pick<PCSEvent, "ct" | "_id">,
  b: Pick<PCSEvent, "ct" | "_id">
) {
  return a.ct.lt(b.ct) || (a.ct.eq(b.ct) && oidLT(a._id, b._id));
}

export async function* dripCEAStart(
  db: Db,
  collectionName: string,
  ccStart: Timestamp,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>,
  options?: Readonly<CEAOptions>
): AsyncGenerator<CSEvent, void, void> {
  const pcseId = z
    .object({ _id: z.instanceof(ObjectId) })
    .optional()
    .parse(
      (
        await db
          .collection(derivePCSCollName(collectionName))
          .find(
            { ct: { $lt: ccStart } },
            { readConcern: ReadConcernLevel.majority }
          )
          .sort({ ct: -1, _id: -1 })
          .project({ _id: 1 })
          .limit(1)
          .toArray()
      )[0]
    )?._id;

  if (!pcseId) {
    throw new CEACursorNotFoundError();
  }

  yield* dripCEAResume(
    db,
    collectionName,
    {
      id: pcseId,
    },
    pipeline,
    processingPipeline,
    options
  );
}

export async function* dripCEAResume(
  db: Db,
  collectionName: string,
  cursor: CEACursor,
  pipeline: Readonly<DripPipeline>,
  processingPipeline?: Readonly<DripProcessingPipeline>,
  options?: Readonly<CEAOptions>
): AsyncGenerator<CSEvent, void, void> {
  const pipelineParsed = parsePipeline(pipeline);
  const pipelineScopedToAfter = synthPipeline(scopeStages(pipelineParsed, "a"));
  const processingPipelineScopedToAfter = processingPipeline
    ? synthPipeline(scopeStages(parsePipeline(processingPipeline), "a"))
    : undefined;
  const pipelineScopedToBefore = scopeStages(stripToGate(pipelineParsed), "b");
  const pipelineScopedToBeforeSynthed = synthPipeline(pipelineScopedToBefore);
  const pipelineScopedToBeforeInverted = invertPipeline(
    pipelineScopedToBefore
  ).map((ppl) => ppl.map(synthStage));
  const coll = db.collection(derivePCSCollName(collectionName));

  const maxCT = z
    .object({
      ct: z.instanceof(Timestamp),
    })
    .optional()
    .parse(
      (
        await coll
          .find({}, { readConcern: ReadConcernLevel.majority })
          .sort({ ct: -1 })
          .limit(1)
          .project({ _id: 0, ct: 1 })
          .toArray()
      )[0]
    )?.ct;

  if (typeof maxCT === "undefined") {
    // No PCS entries yet
    return;
  }

  const cursorEvent = z
    .object({
      ct: z.instanceof(Timestamp),
      w: z.date().optional(),
    })
    .optional()
    .parse(
      (
        await coll
          .find(
            { _id: { $eq: cursor.id } },
            { readConcern: ReadConcernLevel.majority }
          )
          .project({
            _id: 0,
            ct: 1,
            w: 1,
          })
          .toArray()
      )[0]
    );

  if (typeof cursorEvent === "undefined") {
    throw new CEACursorNotFoundError();
  }

  // The reasoning is that the "tail" of the PCS
  // might be incomplete, as the persister does not
  // transactionally insert all PCS events with the
  // same CT.
  // The same potentially also applies to the "head"
  // of the PCS, as the persister might have started
  // in the middle of a cluster time, but CC already
  // ensures that ccStart > PCS head CT.
  if (!cursorEvent.ct.lt(maxCT)) {
    return;
  }

  if (options?.rejectIfOlderThan) {
    const riot = options.rejectIfOlderThan;
    const cursorEventWLB =
      // If the event we have fetched has a wall clock, use it
      // Otherwise, find the closest nop event older than the cursor
      cursorEvent.w ??
      z
        .array(
          z.object({
            w: z.instanceof(Date),
          })
        )
        .parse(
          await coll
            .find(
              // The wall clock only exists for nops
              { o: "n", ct: { $lte: cursorEvent.ct } },
              { readConcern: ReadConcernLevel.majority }
            )
            .sort({ ct: -1 })
            .project({ _id: 0, w: 1 })
            .limit(1)
            .toArray()
        )[0]?.w;

    // Prudently, we assume the cursor might be too old
    // if we don't have a lower bound on its wall clock.
    if (!cursorEventWLB || cursorEventWLB < riot) {
      throw new CEACursorTooOldError();
    }
  }

  // As we use the tuple [clusterTime, id] as the sort order,
  // we need two queries.
  // Or one query with an $or clause, but Mongo has trouble
  // planning them optimally.
  const matchRelevantEvents = [
    {
      ct: {
        $eq: cursorEvent.ct,
      },
      _id: { $gt: cursor.id },
    },
    {
      ct: {
        $lt: maxCT,
        $gt: cursorEvent.ct,
      },
    },
  ];

  const idcta = z.object({
    _id: z.instanceof(ObjectId),
    ct: z.instanceof(Timestamp),
    a: z.record(z.string(), z.unknown()),
  });

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
            ...(processingPipelineScopedToAfter ?? []),
            {
              $project: {
                ct: 1,
                a: 1,
              },
            },
          ],
          { readConcern: ReadConcernLevel.majority }
        )
        .map((x) => {
          return { op: "a" as const, ...idcta.parse(x) };
        })
        [Symbol.asyncIterator]()
    )
  );

  const c2aschema = z
    .object({
      _id: z.instanceof(ObjectId),
      ct: z.instanceof(Timestamp),
    })
    .and(
      z
        .object({
          a: z.never().optional(),
          u: z.record(z.string(), z.unknown()),
          id: z.unknown(),
        })
        .transform((x) => {
          return { op: "u" as const, ...x };
        })
        .or(
          z.object({
            a: z.record(z.string(), z.unknown()),
            u: z.never().optional(),
          })
        )
        .transform((x) => {
          return { op: "r" as const, ...x };
        })
    );

  const c2a = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "u" } },
            ...pipelineScopedToAfter,
            ...pipelineScopedToBeforeSynthed,
            {
              $sort: {
                ct: 1,
                _id: 1,
              },
            },
            // We run the processing pipeline even for
            // update events, which throw away the `a`
            // field anyway, as this is easier than
            // creating another stream just for replacement
            // events.
            ...(processingPipelineScopedToAfter ?? []),
            {
              $project: {
                ct: 1,
                u: 1,
                // If we have the update description, we don't
                // need the after image
                a: {
                  $cond: {
                    if: { $ne: ["$u", "$$REMOVE"] },
                    then: "$$REMOVE",
                    else: "$a",
                  },
                },
                // We don't need the object id for replacements
                // It is in the after image anyway
                id: {
                  $cond: {
                    if: { $ne: ["$u", "$$REMOVE"] },
                    then: "$b._id",
                    else: "$$REMOVE",
                  },
                },
              },
            },
          ],
          { readConcern: ReadConcernLevel.majority }
        )
        .map((x) => {
          return c2aschema.parse(x);
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
              ...(processingPipelineScopedToAfter ?? []),
              {
                $project: {
                  ct: 1,
                  a: 1,
                },
              },
            ],
            { readConcern: ReadConcernLevel.majority }
          )
          .map((x) => {
            return { op: "a" as const, ...idcta.parse(x) };
          })
          [Symbol.asyncIterator]()
      )
    )
  );

  const c2cschema = z.object({
    _id: z.instanceof(ObjectId),
    ct: z.instanceof(Timestamp),
    id: z.unknown(),
  });

  const c2c = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "u" } },
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
                id: "$b._id",
              },
            },
          ],
          { readConcern: ReadConcernLevel.majority }
        )
        .map((x) => {
          return { op: "s" as const, ...c2cschema.parse(x) };
        })
        [Symbol.asyncIterator]()
    )
  );

  const c3schema = z.object({
    _id: z.instanceof(ObjectId),
    ct: z.instanceof(Timestamp),
    id: z.unknown(),
  });

  const c3 = streamAppend(
    matchRelevantEvents.map((mre) =>
      coll
        .aggregate(
          [
            { $match: { ...mre, o: "d" } },
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
                id: "$b._id",
              },
            },
          ],
          { readConcern: ReadConcernLevel.majority }
        )
        .map((x) => {
          return { op: "s" as const, ...c3schema.parse(x) };
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
      // additions due to document updates
      // (might need to be cleaned of updates, we do not invert
      // all pipelines)
      ...c2bs,
      // subtractions due to document updates
      // (when cleaned of updates)
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
          id: cse._id,
        },
        id: cse.id,
        clusterTime: cse.ct,
      } satisfies CSUpdateEvent;
    } else if (cse.op === "r") {
      yield {
        operationType: "replace",
        fullDocument: cse.a,
        cursor: {
          id: cse._id,
        },
        clusterTime: cse.ct,
      } satisfies CSReplaceEvent;
    } else if (cse.op === "a") {
      yield {
        operationType: "addition",
        fullDocument: cse.a,
        cursor: {
          id: cse._id,
        },
        clusterTime: cse.ct,
      } satisfies CSAdditionEvent;
    } else {
      cse.op satisfies "s";
      yield {
        operationType: "subtraction",
        cursor: {
          id: cse._id,
        },
        id: cse.id,
        clusterTime: cse.ct,
      } satisfies CSSubtractionEvent;
    }
  }

  const nopschema = z.object({
    _id: z.instanceof(ObjectId),
    ct: z.instanceof(Timestamp),
  });

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
          .map((x) => nopschema.parse(x))
          .map((x) => {
            return {
              operationType: "noop" as const,
              cursor: {
                id: x._id,
              },
              clusterTime: x.ct,
            } satisfies CSNoopEvent;
          })
          [Symbol.asyncIterator]()
      )
    )
  );
}

export class CEACannotResumeError extends Error {}

export class CEACursorNotFoundError extends CEACannotResumeError {}

export class CEACursorTooOldError extends CEACannotResumeError {}
