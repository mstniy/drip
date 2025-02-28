import { Db, MongoClient, MongoServerError, ObjectId } from "mongodb";
import { after, before, describe, it } from "node:test";
import { openTestDB } from "./open_test_db";
import { startPersister } from "../src/persister/persister";
import { getRandomString } from "./random_string";
import { derivePCSCollName } from "../src/cea/derive_pcs_coll_name";
import {
  PCSDeletionEvent,
  PCSInsertionEvent,
  PCSUpdateEvent,
  zodPCSDeletionEvent,
  zodPCSInsertionEvent,
  zodPCSUpdateEvent,
} from "../src/cea/pcs_event";
import { strict as assert } from "assert";

describe("persister", () => {
  let client: MongoClient;
  let db: Db;
  const collectionName = getRandomString();
  before(async () => {
    [client, db] = await openTestDB();
    await db.createCollection(collectionName);
    await db.command({
      collMod: collectionName,
      changeStreamPreAndPostImages: { enabled: true },
    });
  });
  after(() => client.close());
  it("persists the change stream", async () => {
    startPersister(db, collectionName).catch((e) =>
      // The cursor of course gets killed once the client gets closed
      // at the end of the test
      assert(e instanceof MongoServerError && e.codeName === "CursorKilled")
    );
    // Wait for it to come up and start tracking the change stream
    for (let i = 0; i < 30; i++) {
      await db.collection(collectionName).insertOne({});
      // Check if the persisted change stream collection
      // has been populated
      if (
        (await db
          .collection(derivePCSCollName(collectionName))
          .countDocuments()) > 0
      ) {
        break;
      }
      // Sleep for 250 ms and try again
      await new Promise((res) => setTimeout(res, 250));
    }
    // Give up if the persisted chsange stream collection
    // is still empty.
    if (
      (await db
        .collection(derivePCSCollName(collectionName))
        .countDocuments()) === 0
    ) {
      throw new Error("Persister did not start :(");
    }
    // It is not empty - good, the persister must have started.
    // Do some operations on the main collection and wait for
    // the corresponding PCS entries to appear.
    const id = new ObjectId();
    await db.collection(collectionName).insertOne({ _id: id, a: 0 });
    await db
      .collection(collectionName)
      .updateOne({ _id: id }, { $set: { a: 1 } });
    await db.collection(collectionName).deleteOne({ _id: id });
    // Wait until there are three PCS events for the chosen
    // object id
    for (let i = 0; i < 30; i++) {
      if (
        (await db
          .collection(derivePCSCollName(collectionName))
          .countDocuments({ "k._id": id })) === 3
      ) {
        break;
      }
      // Sleep for 250 ms and try again
      await new Promise((res) => setTimeout(res, 250));
    }
    // Give up if the persisted chsange stream collection
    // does not have three events for the chosen object id
    const pcsEvents = await db
      .collection(derivePCSCollName(collectionName))
      .find({ "k._id": id })
      .sort({ ct: 1 })
      .toArray();
    if (pcsEvents.length !== 3) {
      throw new Error("Persister did not track changes :(");
    }
    const e1 = zodPCSInsertionEvent.parse(pcsEvents[0]);
    const e2 = zodPCSUpdateEvent.parse(pcsEvents[1]);
    const e3 = zodPCSDeletionEvent.parse(pcsEvents[2]);

    assert.deepStrictEqual(e1, {
      v: 1,
      o: "i",
      _id: e1._id,
      a: { _id: id, a: 0 },
      ct: e1.ct,
      k: { _id: id },
      w: e1.w,
    } satisfies PCSInsertionEvent);

    assert.deepStrictEqual(e2, {
      v: 1,
      o: "u",
      _id: e2._id,
      b: { _id: id, a: 0 },
      a: { _id: id, a: 1 },
      u: {
        i: {
          a: 1,
        },
      },
      ct: e2.ct,
      k: { _id: id },
      w: e2.w,
    } satisfies PCSUpdateEvent);

    assert.deepStrictEqual(e3, {
      v: 1,
      o: "d",
      _id: e3._id,
      b: { _id: id, a: 1 },
      ct: e3.ct,
      k: { _id: id },
      w: e3.w,
    } satisfies PCSDeletionEvent);
  });
});
