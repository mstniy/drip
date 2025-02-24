import { after, before, describe, it } from "node:test";
import { getRandomString } from "./random_string";
import { BSON, Db, Document, MongoClient } from "mongodb";
import { openTestDB } from "./open_test_db";
import { genToArray } from "./gen_to_array";
import { dripCC, dripCCRaw } from "../src/cc/cc";
import { strict as assert } from "assert";

describe("dripCC", () => {
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
  it("works without a cursor", async () => {
    const res = await genToArray(
      dripCC(db, collectionName, { stages: [{ $match: { a: 0 } }] })
    );

    assert.deepStrictEqual(res, [
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
  it("works with a cursor", async () => {
    const res = await genToArray(
      dripCC(db, { collectionName, id: 0 }, { stages: [{ $match: { a: 0 } }] })
    );

    assert.deepStrictEqual(res, [
      { _id: 2, a: 0 },
      {
        _id: 3,
        a: 0,
      },
    ]);
  });
  describe("dripCCRaw", () => {
    it("works without a cursor", async () => {
      const res = (
        await genToArray(
          dripCCRaw(db, collectionName, { stages: [{ $match: { a: 0 } }] })
        )
      ).map((x) => BSON.deserialize(new Uint8Array(x.buffer)));

      assert.deepStrictEqual(res, [
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
    it("works with a cursor", async () => {
      const res = (
        await genToArray(
          dripCCRaw(
            db,
            { collectionName, id: 0 },
            { stages: [{ $match: { a: 0 } }] }
          )
        )
      ).map((x) => BSON.deserialize(new Uint8Array(x.buffer)));

      assert.deepStrictEqual(res, [
        { _id: 2, a: 0 },
        {
          _id: 3,
          a: 0,
        },
      ]);
    });
  });
});
