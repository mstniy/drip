import { Db, Timestamp, UUID } from "mongodb";
import { CSAdditionEvent, CSEvent } from "./cs_event";
import { Rule } from "./rule";
import z from "zod";
import { zodPCSInsertionEvent, zodPCSUpdateEvent } from "./pcs_event";
import { CEACursor } from "./cea_cursor";

export async function* dripCEAStart(
  db: Db,
  _ns: String,
  syncStart: Date,
  rule: Rule
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

  console.log(minCT);

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
    rule
  );
}

export async function* dripCEAResume(
  db: Db,
  cursor: CEACursor,
  rule: Rule
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

  const c1 = coll
    .aggregate([
      {
        $match: {
          o: { $in: ["u", "i"] },
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
      },
      ...rule.stages,
      {
        $sort: {
          ct: 1,
          _id: 1,
        },
      },
    ])
    .map((x) => zodPCSInsertionEvent.or(zodPCSUpdateEvent).parse(x))
    .map((x) => {
      return {
        operationType: "addition",
        fullDocument: x.a,
        cursor: {
          collectionUUID: cursor.collectionUUID,
          clusterTime: x.ct,
          id: x._id,
        },
      } satisfies CSAdditionEvent;
    });

  for await (const event of c1) {
    yield event;
  }
}

/* 

export async function* dripCCRaw(
  db: Db,
  nsOrCursor: String | CCCursor,
  rule: Rule
): AsyncGenerator<Buffer, void, void> {}

export async function* dripCC(
  db: Db,
  nsOrCursor: String | CCCursor,
  rule: Rule
): AsyncGenerator<Document, void, void> {} */
