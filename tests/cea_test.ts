import { after, before, describe, it } from "node:test";
import { Collection, Db, MongoClient, ObjectId, Timestamp } from "mongodb";

import { dripCEAStart } from "../src/drip";
import { strict as assert } from "assert";
import { PCSInsertionEvent, PCSNoopEvent } from "../src/cea/pcs_event";
import { genToArray } from "./gen_to_array";
import { zodCSUpsertEvent } from "./schemas/cs_events";
import { SyncStartTooOldError } from "../src/cea/cea";

const TEST_DB_NAME = "drip_test";

async function reinitDB() {
  const client = new MongoClient("mongodb://127.0.0.1:27017");
  await client.connect();
  const db = client.db(TEST_DB_NAME);
  assert(db.databaseName === TEST_DB_NAME);
  await db.dropDatabase();

  return [client, db, db.collection("_drip_pcs_dripCEAStart")] as const;
}

describe("dripCEAStart", () => {
  let client: MongoClient;
  let db: Db;
  let coll: Collection;
  before(async () => ([client, db, coll] = await reinitDB()));
  after(() => client.close());
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
  before(() => coll.insertMany(events));
  it("ignores too old events", async () => {
    const res_ = await genToArray(
      dripCEAStart(
        db,
        "dripCEAStart",
        events[1].w,
        { stages: [] },
        { stages: [] }
      )
    );

    assert.equal(res_.length, 1);
    const res = zodCSUpsertEvent.parse(res_[0]);

    assert.deepStrictEqual(res.fullDocument, events[1].a);
    assert(res.cursor.clusterTime.equals(events[1].ct));
    assert(res.cursor.id!.equals(events[1]._id));
  });

  it("returns no results if given time is too recent", async () => {
    const res = await genToArray(
      dripCEAStart(
        db,
        "dripCEAStart",
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
          "dripCEAStart",
          new Date(
            events[0].w.setUTCFullYear(events[0].w.getUTCFullYear() - 1)
          ),
          { stages: [] },
          { stages: [] }
        )
      );
      assert(false, "Must have thrown");
    } catch (e) {
      assert(e instanceof SyncStartTooOldError);
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
