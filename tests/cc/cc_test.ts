import { after, before, describe, it } from "node:test";
import {
  BSON,
  ClusterTime,
  Db,
  Document,
  Long,
  MongoClient,
  ObjectId,
  Timestamp,
} from "mongodb";
import { strict as assert } from "assert";
import { CCWaitForPersisterError, dripCC, dripCCRaw } from "../../src";
import { genToArray } from "../test_utils/gen_to_array";
import { openTestDB } from "../test_utils/open_test_db";
import { getRandomString } from "../test_utils/random_string";
import { PCSNoopEvent } from "../../src/cea/pcs_event";
import { derivePCSCollName } from "../../src/cea/derive_pcs_coll_name";

const collectionName = getRandomString();
let client: MongoClient;
let db: Db, mddb: Db;
const objs = [
  {
    _id: 2,
    a: 0,
  },
  {
    _id: 1,
    a: 1,
  },
  {
    _id: 3,
    a: 0,
  },
  {
    _id: 0,
    a: 0,
  },
] satisfies Document[];
// Create an old noop event to avoid
// CC failing due to a lack of PCS events.
const events = [
  {
    _id: new ObjectId(),
    ct: new Timestamp({ i: 0, t: 0 }),
    o: "n",
    w: new Date(),
  },
] satisfies PCSNoopEvent[];
before(async () => {
  [client, db, mddb] = await openTestDB();
  await db.collection<{ _id: number }>(collectionName).insertMany(objs);
  await mddb.collection(derivePCSCollName(collectionName)).insertMany(events);
});
after(() => client.close());

describe("dripCC", () => {
  it("works without a cursor", async () => {
    const res = await genToArray(
      dripCC(
        client,
        db.databaseName,
        mddb.databaseName,
        collectionName,
        undefined,
        [{ $match: { a: 0 } }]
      )
    );

    const docs = res.flatMap((r) => r[1]);

    assert.deepStrictEqual(docs, [
      {
        _id: 0,
        a: 0,
      },
      { _id: 2, a: 0 },
      {
        _id: 3,
        a: 0,
      },
    ]);
  });
  it("yields a cluster time even if there are no documents", async () => {
    // Operate on a separate collection to avoid
    // interfering with the other tests.
    const collectionName = getRandomString();
    await mddb.collection(derivePCSCollName(collectionName)).insertMany(events);
    const res = await genToArray(
      dripCC(
        client,
        db.databaseName,
        mddb.databaseName,
        collectionName,
        undefined,
        []
      )
    );
    // One for the lower bound on the start,
    // another for the upper bound on the end.
    assert.equal(res.length, 2);
  });
  it("throws if there are no prior persisted change events", async () => {
    // Operate on a separate collection to avoid
    // interfering with the other tests.
    const collectionName = getRandomString();
    // Create one too new change event
    await mddb
      .collection(derivePCSCollName(collectionName))
      .insertOne({ ...events[0], ct: new Timestamp(Long.MAX_VALUE) });
    try {
      await genToArray(
        dripCC(
          client,
          db.databaseName,
          mddb.databaseName,
          collectionName,
          undefined,
          []
        )
      );
      throw new Error("must have thrown");
    } catch (e) {
      assert(e instanceof CCWaitForPersisterError);
    }
  });
  it("works with a cursor", async () => {
    // Get a cluster time
    const cctmp = dripCC(
      client,
      db.databaseName,
      mddb.databaseName,
      collectionName,
      undefined,
      []
    );
    let ccStart: ClusterTime | undefined;
    for await (const x of cctmp) {
      ccStart = x[0];
      break;
    }
    assert(ccStart, "dripCC did not return a cluster time");
    const res = await genToArray(
      dripCC(
        client,
        db.databaseName,
        mddb.databaseName,
        collectionName,
        [{ id: 0 }, ccStart],
        [{ $match: { a: 0 } }]
      )
    );

    const docs = res.flatMap((r) => r[1]);

    assert.deepStrictEqual(docs, [
      { _id: 2, a: 0 },
      {
        _id: 3,
        a: 0,
      },
    ]);
  });
});

describe("dripCCRaw", () => {
  it("works", async () => {
    const res = await genToArray(
      dripCCRaw(
        client,
        db.databaseName,
        mddb.databaseName,
        collectionName,
        undefined,
        [{ $match: { a: 0 } }]
      )
    );

    const docs = res.flatMap((r) => r[1]).map((x) => BSON.deserialize(x));

    assert.deepStrictEqual(docs, [
      {
        _id: 0,
        a: 0,
      },
      { _id: 2, a: 0 },
      {
        _id: 3,
        a: 0,
      },
    ]);
  });
});
