import { after, before, describe, it } from "node:test";
import { BSON, Db, Document, MongoClient } from "mongodb";
import { strict as assert } from "assert";
import {
  dripCCRawResume,
  dripCCRawStart,
  dripCCResume,
  dripCCStart,
} from "../../src";
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
describe("dripCCStart", () => {
  it("works", async () => {
    const { gen } = await dripCCStart(db, collectionName, [
      { $match: { a: 0 } },
    ]);
    const res = await genToArray(gen);

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
});
describe("dripCCResume", () => {
  it("works", async () => {
    const res = await genToArray(
      dripCCResume(db, collectionName, { id: 0 }, [{ $match: { a: 0 } }])
    );

    assert.deepStrictEqual(res, [
      { _id: 2, a: 0 },
      {
        _id: 3,
        a: 0,
      },
    ]);
  });
});

describe("dripCCRawStart", () => {
  it("works", async () => {
    const { gen } = await dripCCRawStart(db, collectionName, [
      { $match: { a: 0 } },
    ]);
    const res = (await genToArray(gen)).map((x) => BSON.deserialize(x));

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
});
describe("dripCCRawResume", () => {
  it("works", async () => {
    const res = (
      await genToArray(
        dripCCRawResume(db, collectionName, { id: 0 }, [{ $match: { a: 0 } }])
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
