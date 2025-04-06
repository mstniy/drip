import { after, before, describe, it } from "node:test";
import { BSON, ClusterTime, Db, Document, MongoClient } from "mongodb";
import { strict as assert } from "assert";
import { dripCC, dripCCRaw } from "../../src";
import { genToArray } from "../test_utils/gen_to_array";
import { openTestDB } from "../test_utils/open_test_db";
import { getRandomString } from "../test_utils/random_string";

const collectionName = getRandomString();
let client: MongoClient;
let db: Db;
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
before(async () => {
  [client, db] = await openTestDB();
  await db.collection<{ _id: number }>(collectionName).insertMany(objs);
});
after(() => client.close());

describe("dripCC", () => {
  it("works without a cursor", async () => {
    const res = await genToArray(
      dripCC(client, db.databaseName, collectionName, undefined, [
        { $match: { a: 0 } },
      ])
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
    const res = await genToArray(
      dripCC(client, db.databaseName, "nosuchcollection", undefined, [])
    );
    // One for the lower bound on the start,
    // another for the upper bound on the end.
    assert.equal(res.length, 2);
  });
  it("works with a cursor", async () => {
    // Get a cluster time
    const cctmp = dripCC(
      client,
      db.databaseName,
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
      dripCCRaw(client, db.databaseName, collectionName, undefined, [
        { $match: { a: 0 } },
      ])
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
