import { after, before, describe, it } from "node:test";
import { BSON, Db, Document, MongoClient } from "mongodb";
import { strict as assert } from "assert";
import { dripCC, dripCCRaw } from "../../src";
import { genToArray } from "../test_utils/gen_to_array";
import { openTestDB } from "../test_utils/open_test_db";
import { getRandomString } from "../test_utils/random_string";

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
      dripCC(db, collectionName, [{ $match: { a: 0 } }])
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
      dripCC(db, { collectionName, id: 0 }, [{ $match: { a: 0 } }])
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
        await genToArray(dripCCRaw(db, collectionName, [{ $match: { a: 0 } }]))
      ).map((x) => BSON.deserialize(x));

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
          dripCCRaw(db, { collectionName, id: 0 }, [{ $match: { a: 0 } }])
        )
      ).map((x) => BSON.deserialize(x));

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
