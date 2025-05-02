import { MongoClient, Db, ObjectId, Timestamp, Collection } from "mongodb";
import { after, before, beforeEach, describe, it } from "node:test";
import { openTestDB } from "../test_utils/open_test_db";
import { getRandomString } from "../test_utils/random_string";
import { PCSNoopEvent, PCSInsertionEvent } from "../../src/cea/pcs_event";
import { derivePCSCollName } from "../../src/cea/derive_pcs_coll_name";
import { expirePCSEvents } from "../../src/cleaner/cleaner";
import {
  advanceDate,
  incrementDate,
  ONE_YEAR_MS,
} from "../test_utils/date_utils";
import { strict as assert } from "assert";

describe("cleaner", () => {
  let client: MongoClient;
  let db: Db;
  let collectionName: string;
  let pcsCollection: Collection;
  const events = [
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050683, i: 0 }),
      o: "n",
      w: new Date("2025-02-20T11:23:44.707Z"),
    } satisfies PCSNoopEvent,
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050684, i: 0 }),
      k: {
        _id: "a",
      },
      o: "i",
      a: {
        _id: "a",
        a: 0,
      },
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050685, i: 0 }),
      o: "n",
      w: new Date("2025-02-20T11:24:44.708Z"),
    } satisfies PCSNoopEvent,
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050685, i: 1 }),
      k: {
        _id: "b",
      },
      o: "i",
      a: {
        _id: "b",
        a: 0,
      },
    } satisfies PCSInsertionEvent,
    {
      _id: new ObjectId(),
      ct: new Timestamp({ t: 1740050685, i: 2 }),
      o: "n",
      w: new Date("2025-02-20T11:25:44.708Z"),
    },
  ] as const;
  before(async () => {
    [client, , db] = await openTestDB();
  });
  after(() => client.close());

  beforeEach(async () => {
    collectionName = getRandomString();
    pcsCollection = db.collection(derivePCSCollName(collectionName));
    await pcsCollection.insertMany(events);
  });

  it("does nothing if there are no persisted events", async () => {
    await expirePCSEvents(
      "nosuchcollection",
      client,
      db.databaseName,
      new Date()
    );
  });
  it("does nothing if there are no affected events", async () => {
    await expirePCSEvents(
      collectionName,
      client,
      db.databaseName,
      advanceDate(events[0].w, -ONE_YEAR_MS)
    );
    assert.deepStrictEqual(
      (await pcsCollection.find().sort({ ct: 1 }).toArray()).map((o) => o._id),
      events.map((o) => o._id)
    );
  });
  it("does not clean the tail of the PCS", async () => {
    await expirePCSEvents(
      collectionName,
      client,
      db.databaseName,
      incrementDate(events[4].w)
    );
    assert.deepStrictEqual(
      (await pcsCollection.find().sort({ ct: 1 }).toArray()).map((o) => o._id),
      [events[4]._id]
    );
  });
  it("deletes the affected events", async () => {
    await expirePCSEvents(collectionName, client, db.databaseName, events[4].w);
    assert.deepStrictEqual(
      (await pcsCollection.find().sort({ ct: 1 }).toArray()).map((o) => o._id),
      events.slice(3).map((o) => o._id)
    );
  });
});
