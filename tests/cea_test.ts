import { after, before, describe, it } from "node:test";
import { Db, MongoClient, ObjectId, Timestamp } from "mongodb";

import {
  CSAdditionEvent,
  CSSubtractionEvent,
  CSUpdateEvent,
  dripCEAStart,
} from "../src/drip";
import { strict as assert } from "assert";
import {
  PCSDeletionEvent,
  PCSInsertionEvent,
  PCSNoopEvent,
  PCSUpdateEvent,
} from "../src/cea/pcs_event";
import { genToArray } from "./gen_to_array";
import { dripCEAResume, CEACursorNotFoundError } from "../src/cea/cea";
import { getRandomString } from "./random_string";
import { minOID } from "../src/cea/min_oid";

const TEST_DB_NAME = "drip_test";

async function openTestDB() {
  const client = new MongoClient("mongodb://127.0.0.1:27017");
  await client.connect();
  const db = client.db(TEST_DB_NAME);

  return [client, db] as const;
}

describe("dripCEAStart", () => {
  const collectionName = getRandomString();
  let client: MongoClient;
  let db: Db;
  const events = [
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050684, i: 0 }),
      k: {
        _id: "a",
      },
      w: new Date("2025-02-20T11:24:44.706Z"),
      o: "i",
      a: {
        _id: "a",
        a: 0,
      },
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050684, i: 1 }),
      k: {
        _id: "b",
      },
      w: new Date("2025-02-20T11:24:44.707Z"),
      o: "i",
      a: {
        _id: "b",
        a: 0,
      },
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050684, i: 2 }),
      o: "n",
      w: new Date("2025-02-20T11:24:44.708Z"),
    } satisfies PCSNoopEvent,
  ] as const;
  before(async () => {
    [client, db] = await openTestDB();
    await db.collection(`_drip_pcs_${collectionName}`).insertMany(events);
  });
  after(() => client.close());
  it("ignores too old events", async () => {
    const res = await genToArray(
      dripCEAStart(
        db,
        collectionName,
        events[1].w,
        { stages: [] },
        { stages: [] }
      )
    );

    assert.deepStrictEqual(res, [
      {
        cursor: {
          clusterTime: events[1].ct,
          id: events[1]._id,
          collectionName,
        },
        fullDocument: events[1].a,
        operationType: "addition",
      } satisfies CSAdditionEvent,
    ]);
  });

  it("returns no results if given time is too recent", async () => {
    const res = await genToArray(
      dripCEAStart(
        db,
        collectionName,
        new Date(events[2].w.setUTCFullYear(events[2].w.getUTCFullYear() + 1)),
        { stages: [] },
        { stages: [] }
      )
    );
    assert.equal(res.length, 0);
  });

  it("throws if given time too old", async () => {
    try {
      await genToArray(
        dripCEAStart(
          db,
          collectionName,
          new Date(
            events[0].w.setUTCFullYear(events[0].w.getUTCFullYear() - 1)
          ),
          { stages: [] },
          { stages: [] }
        )
      );
      assert(false, "Must have thrown");
    } catch (e) {
      assert(e instanceof CEACursorNotFoundError);
    }
  });
  it("yields nothing if there are no persisted events", async () => {
    const res = await genToArray(
      dripCEAStart(
        db,
        "no_such_collection",
        new Date(),
        { stages: [] },
        { stages: [] }
      )
    );
    assert(res.length === 0);
  });
});

describe("dripCEAResume", () => {
  let client: MongoClient;
  let db: Db;
  let collectionName: string;
  const events = [
    {
      _id: new ObjectId(),
      a: { _id: "a", a: 0 },
      ct: new Timestamp({ t: 1740050684, i: 0 }),
      w: new Date(),
      k: { _id: "a" },
      o: "i",
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      a: { _id: "b", a: 0 },
      ct: new Timestamp({ t: 1740050685, i: 0 }),
      w: new Date(),
      k: { _id: "b" },
      o: "i",
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      a: { _id: "c", a: 1 },
      ct: new Timestamp({ t: 1740050685, i: 2 }),
      w: new Date(),
      k: { _id: "c" },
      o: "i",
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      b: { _id: "d", a: 0 },
      ct: new Timestamp({ t: 1740050685, i: 3 }),
      w: new Date(),
      k: { _id: "d" },
      o: "d",
    } satisfies PCSDeletionEvent,
    {
      _id: new ObjectId(),
      b: { _id: "e", a: 1 },
      ct: new Timestamp({ t: 1740050685, i: 4 }),
      w: new Date(),
      k: { _id: "e" },
      o: "d",
    } satisfies PCSDeletionEvent,
    {
      _id: new ObjectId(),
      a: { _id: "a", a: 0, b: 1 },
      b: { _id: "a", a: 0 },
      u: {}, // TODO: Fill these out
      ct: new Timestamp({ t: 1740050686, i: 0 }),
      w: new Date(),
      k: { _id: "a" },
      o: "u",
    } satisfies PCSUpdateEvent,
    {
      _id: new ObjectId(),
      a: { _id: "b", a: 1 },
      b: { _id: "b", a: 0 },
      u: {},
      ct: new Timestamp({ t: 1740050686, i: 1 }),
      w: new Date(),
      k: { _id: "b" },
      o: "u",
    } satisfies PCSUpdateEvent,
    {
      _id: new ObjectId(),
      a: { _id: "c", a: 0 },
      b: { _id: "c", a: 1 },
      u: {},
      ct: new Timestamp({ t: 1740050687, i: 0 }),
      w: new Date(),
      k: { _id: "c" },
      o: "u",
    } satisfies PCSUpdateEvent,
    {
      _id: new ObjectId(),
      a: { _id: "f", a: 2 },
      b: { _id: "f", a: 1 },
      u: {},
      ct: new Timestamp({ t: 1740050687, i: 1 }),
      w: new Date(),
      k: { _id: "f" },
      o: "u",
    } satisfies PCSUpdateEvent,
    {
      _id: new ObjectId(),
      a: { _id: "g", a: 0 },
      ct: new Timestamp({ t: 1740050688, i: 0 }),
      w: new Date(),
      k: { _id: "g" },
      o: "i",
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      a: { _id: "h", a: 0 },
      ct: new Timestamp({ t: 1740050688, i: 0 }),
      w: new Date(),
      k: { _id: "h" },
      o: "i",
    } satisfies PCSInsertionEvent,
  ] as const;
  before(async () => {
    [client, db] = await openTestDB();
    collectionName = getRandomString();
    await db.collection(`_drip_pcs_${collectionName}`).insertMany(events);
  });
  after(() => client.close());
  it("yields nothing if there are no persisted events", async () => {
    const res = await genToArray(
      dripCEAResume(
        db,
        {
          clusterTime: new Timestamp({ t: 1000, i: 0 }),
          collectionName: "no_such_collection",
          id: minOID,
        },
        { stages: [] },
        { stages: [] }
      )
    );
    assert.equal(res.length, 0);
  });
  it("throws if passed the smallest cluster time or smaller", async (t) => {
    for (let [testName, ct] of [
      ["smaller", new Timestamp({ t: 1740050683, i: 0 })],
      ["smallest", new Timestamp({ t: 1740050684, i: 0 })],
    ] as const) {
      await t.test(testName, async () => {
        try {
          await genToArray(
            dripCEAResume(
              db,
              {
                clusterTime: ct,
                collectionName,
                id: minOID,
              },
              { stages: [] },
              { stages: [] }
            )
          );
          assert(false, "must have thrown");
        } catch (e) {
          assert(e instanceof CEACursorNotFoundError);
        }
      });
    }
  });
  it("starts at the given cursor", async () => {
    const res = await genToArray(
      dripCEAResume(
        db,
        {
          clusterTime: events[7].ct,
          collectionName,
          id: events[7]._id,
        },
        { stages: [] },
        { stages: [] }
      )
    );
    assert.deepStrictEqual(res, [
      {
        cursor: {
          clusterTime: events[8].ct,
          collectionName,
          id: events[8]._id,
        },
        updateDescription: {},
        operationType: "update",
      } satisfies CSUpdateEvent,
    ]);
  });
  it("converts the PCS to subset events", async () => {
    const res = await genToArray(
      dripCEAResume(
        db,
        {
          clusterTime: events[1].ct,
          collectionName,
          id: minOID,
        },
        { stages: [{ $match: { "a.a": 0 } }] },
        { stages: [{ $match: { "b.a": 0 } }] }
      )
    );

    assert.deepStrictEqual(res, [
      // insertion of a relevant object is an addition
      {
        cursor: {
          clusterTime: events[1].ct,
          collectionName,
          id: events[1]._id,
        },
        fullDocument: events[1].a,
        operationType: "addition",
      } satisfies CSAdditionEvent,
      // events[2] is omitted: irrelevant insertion
      // deletion of a relevant object is a subtraction
      {
        cursor: {
          clusterTime: events[3].ct,
          collectionName,
          id: events[3]._id,
        },
        id: events[3].b._id,
        operationType: "subtraction",
      } satisfies CSSubtractionEvent,
      // events[4] is omitted: irrelevant deletion
      // update of a relevant object is an update, if it stays relevant
      {
        cursor: {
          clusterTime: events[5].ct,
          collectionName,
          id: events[5]._id,
        },
        updateDescription: {},
        operationType: "update",
      } satisfies CSUpdateEvent,
      // update of a relevant object is a sutraction, if it is not relevant anymore
      {
        cursor: {
          clusterTime: events[6].ct,
          collectionName,
          id: events[6]._id,
        },
        id: events[6].b._id,
        operationType: "subtraction",
      } satisfies CSSubtractionEvent,
      // update of an irrelevant object is an addition, if it was not relevant
      {
        cursor: {
          clusterTime: events[7].ct,
          collectionName,
          id: events[7]._id,
        },
        fullDocument: events[7].a,
        operationType: "addition",
      } satisfies CSAdditionEvent,
      // events[8] is omitted: it is an update to an irrelevant object, which is still irrelevant
      // events[9] and events[10] are omitted: they have the largest cluster time, so we ignore them
    ]);
  });
});
