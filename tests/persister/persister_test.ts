import {
  Db,
  Document,
  MongoClient,
  MongoServerError,
  ObjectId,
  ReadConcernLevel,
  Timestamp,
} from "mongodb";
import { afterEach, beforeEach, describe, it } from "bun:test";
import { strict as assert } from "assert";
import { derivePCSCollName } from "../../src/cea/derive_pcs_coll_name";
import { DripMetadata, MetadataCollectionName } from "../../src/cea/metadata";
import {
  zodPCSInsertionEvent,
  zodPCSUpdateEvent,
  zodPCSDeletionEvent,
  PCSInsertionEvent,
  PCSUpdateEvent,
  PCSDeletionEvent,
} from "../../src/cea/pcs_event";
import { runPersister } from "../../src/persister/persister";
import { openTestDB } from "../test_utils/open_test_db";
import { getRandomString } from "../test_utils/random_string";
import z from "zod";
import { decodeResumeToken } from "mongodb-resumetoken-decoder";
import { sleep } from "../test_utils/sleep";

describe("persister", () => {
  let client: MongoClient;
  let db: Db, mddb: Db;
  let collectionName: string;
  beforeEach(async () => {
    collectionName = getRandomString();
    [client, db, mddb] = await openTestDB();
    await db.createCollection(collectionName);
    await db.command({
      collMod: collectionName,
      changeStreamPreAndPostImages: { enabled: true },
    });
  });
  afterEach(() => client.close());

  async function runPersisterDetached() {
    const persister = runPersister(
      client,
      mddb.databaseName,
      db.collection(collectionName)
    );
    while (true) {
      await persister.next();
    }
  }

  async function test() {
    runPersisterDetached().catch((e) =>
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
        (await mddb
          .collection(derivePCSCollName(collectionName))
          .countDocuments()) > 0
      ) {
        break;
      }
      // Sleep for 250 ms and try again
      await sleep(250);
    }
    // Give up if the persisted chsange stream collection
    // is still empty.
    if (
      (await mddb
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
        (await mddb
          .collection(derivePCSCollName(collectionName))
          .countDocuments({ "k._id": id })) === 3
      ) {
        break;
      }
      // Sleep for 250 ms and try again
      await sleep(250);
    }
    let pcsEvents!: Document[];
    let mdEntry!: Document | undefined;
    // Read the PCS and the metadata table transactionally
    // using the snapshot read concern, as we'll compare
    // their states with one another.
    await client.withSession((session) =>
      session.withTransaction(
        async (session) => {
          pcsEvents = await mddb
            .collection(derivePCSCollName(collectionName))
            .find({ "k._id": id }, { session })
            .sort({ ct: 1 })
            .toArray();
          mdEntry = (
            await mddb
              .collection<{ _id: string }>(MetadataCollectionName)
              .find({ _id: collectionName }, { session })
              .toArray()
          )[0];
        },
        { readConcern: ReadConcernLevel.snapshot }
      )
    );
    if (pcsEvents.length !== 3) {
      throw new Error("Persister did not track changes :(");
    }
    const e1 = zodPCSInsertionEvent.parse(pcsEvents[0]);
    const e2 = zodPCSUpdateEvent.parse(pcsEvents[1]);
    const e3 = zodPCSDeletionEvent.parse(pcsEvents[2]);

    assert.deepStrictEqual(e1, {
      o: "i",
      _id: e1._id,
      a: { _id: id, a: 0 },
      ct: e1.ct,
      k: { _id: id },
    } satisfies PCSInsertionEvent);

    assert.deepStrictEqual(e2, {
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
    } satisfies PCSUpdateEvent);

    assert.deepStrictEqual(e3, {
      o: "d",
      _id: e3._id,
      b: { _id: id, a: 1 },
      ct: e3.ct,
      k: { _id: id },
    } satisfies PCSDeletionEvent);

    // The resume token must exist
    const resumeTokenData = z
      .object({ resumeToken: z.object({ _data: z.string() }) })
      .parse(mdEntry).resumeToken._data;
    // mongodb-resumetoken-decoder and the actual driver use
    // incompatible bson versions, so translate between
    // the two
    const decoded = decodeResumeToken(resumeTokenData);
    const rtCT = Timestamp.fromBits(
      decoded.timestamp.low,
      decoded.timestamp.high
    );
    // The resume token must point to the last persisted event
    assert(rtCT.eq(e3.ct));
  }

  it("works when there is no resume token", test);

  it("works when there is a saved resume token", async () => {
    // First we need a valid resume token
    const wc = db.collection(collectionName).watch();
    let completer: (_: void) => void;
    const completerPromise = new Promise((res) => (completer = res));
    let resumeToken: unknown;
    wc.once("resumeTokenChanged", (rt) => {
      resumeToken = rt;
      completer();
    });
    // Or else the driver won't actually start
    // the change stream
    wc.once("change", () => null);
    await completerPromise;
    await mddb.collection<DripMetadata>(MetadataCollectionName).insertOne({
      _id: collectionName,
      resumeToken,
    } satisfies DripMetadata);
    await test();
  });

  it("throws when an invalid resume token is saved", async () => {
    await mddb.collection<DripMetadata>(MetadataCollectionName).insertOne({
      _id: collectionName,
      resumeToken: "not a valid resume token :(",
    } satisfies DripMetadata);

    try {
      await runPersisterDetached();
      throw new Error("must have thrown :(");
    } catch (e) {
      assert(
        e instanceof MongoServerError &&
          e.message ===
            "BSON field '$changeStream.resumeAfter' is the wrong type 'string', expected type 'object'"
      );
    }
  });

  it("can stop gracefully", async () => {
    // Use a new collection to avoid
    // interfering with the other tests
    const collectionName = getRandomString();

    const p = runPersister(
      client,
      mddb.databaseName,
      db.collection(collectionName),
      {
        // As otherwise is takes ~10 seconds for the
        // tryNext() call to return, and the test also
        // takes ~10 seconds to finish.
        maxAwaitTimeMS: 10,
      }
    );
    // Let it run for a while
    for (let i = 0; i < 10; i++) {
      const res = await p.next();
      // Does not terminate by itself
      assert(!res.done);
      // Wait for a task for interesting things to happen
      await sleep(0);
    }
    // Shut it down
    await p.return();

    // Not we can close the client
    await client.close();
  });
});
