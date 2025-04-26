import {
  MongoClient,
  Db,
  MongoServerError,
  Collection,
  Timestamp,
  ObjectId,
} from "mongodb";
import { before, afterEach, beforeEach, describe, it } from "node:test";
import { runPersister } from "../src/persister/persister";
import { openTestDB } from "../tests/test_utils/open_test_db";
import { getRandomString } from "../tests/test_utils/random_string";
import { strict as assert, AssertionError } from "assert";
import z from "zod";
import { MetadataCollectionName } from "../src/cea/metadata";
import { derivePCSCollName } from "../src/cea/derive_pcs_coll_name";
import { sleep } from "../tests/test_utils/sleep";
import { PCSEventCommon, zodPCSEventCommon } from "../src/cea/pcs_event";

function assertNonDescreasingCT(buffer: PCSEventCommon[]) {
  buffer
    .map((pcse) => pcse.ct)
    .reduce((a, b) => {
      assert(a.lte(b), "must be non-decreasing");
      return b;
    });
}

// This assumes the buffer is non-decreasing
function assertUniqueNoops(buffer: PCSEventCommon[]) {
  buffer
    // Only keep the noops
    .filter((x) => x.o === "n")
    .reduce((a, b) => {
      assert(b.ct.t > a.ct.t, "must be unique");
      return b;
    });
}

function checkPCSInvariants(buffer: PCSEventCommon[]) {
  // Assert that the PCS events were created in
  // non-decreasing CT order
  assertNonDescreasingCT(buffer);

  // Assert that noops are unique
  // per cluster time t field
  assertUniqueNoops(buffer);
}

describe("self test", () => {
  describe("assertNonDescreasingCT", () => {
    it("fails for an empty list", () => {
      try {
        assertNonDescreasingCT([]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(
          e instanceof TypeError &&
            e.message === "Reduce of empty array with no initial value"
        );
      }
    });
    it("passes for a list with one element", () => {
      assertNonDescreasingCT([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
      ]);
    });
    it("passes for a list with two elements of the same CT", () => {
      assertNonDescreasingCT([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
      ]);
    });
    it("passes for a list with two elements of increasing CT", () => {
      assertNonDescreasingCT([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 1 }),
          o: "n",
        },
      ]);
    });
    it("fails for a list with decreasing CT", () => {
      try {
        assertNonDescreasingCT([
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 1 }),
            o: "n",
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
          },
        ]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(
          e instanceof AssertionError && e.message === "must be non-decreasing"
        );
      }
    });
  });
  describe("assertUniqueNoops", () => {
    it("fails for an empty list", () => {
      try {
        assertUniqueNoops([]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(
          e instanceof TypeError &&
            e.message === "Reduce of empty array with no initial value"
        );
      }
    });
    it("fails for a list with no noops", () => {
      try {
        assertUniqueNoops([
          {
            _id: new ObjectId(),
            o: "d",
            ct: new Timestamp({ t: 0, i: 0 }),
          },
        ]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(
          e instanceof TypeError &&
            e.message === "Reduce of empty array with no initial value"
        );
      }
    });
  });
  it("passes for a list with one noop", () => {
    assertUniqueNoops([
      {
        _id: new ObjectId(),
        ct: new Timestamp({ t: 0, i: 0 }),
        o: "n",
      },
    ]);
  });
  it("passes for a list with nops on unique t-s", () => {
    assertUniqueNoops([
      {
        _id: new ObjectId(),
        ct: new Timestamp({ t: 0, i: 0 }),
        o: "n",
      },
      {
        _id: new ObjectId(),
        ct: new Timestamp({ t: 1, i: 0 }),
        o: "n",
      },
      {
        _id: new ObjectId(),
        ct: new Timestamp({ t: 2, i: 0 }),
        o: "n",
      },
    ]);
  });
  it("fails for a list with nops on identical t-s", () => {
    const cases = [
      [
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 1, i: 0 }),
          o: "n",
        },
      ],
      [
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 1, i: 0 }),
          o: "n",
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 1, i: 0 }),
          o: "n",
        },
      ],
    ];
    for (const c of cases) {
      try {
        assertUniqueNoops(c);
        assert(false, "must have thrown");
      } catch (e) {
        assert(e instanceof AssertionError && e.message === "must be unique");
      }
    }
  });
});

describe("persister", () => {
  let client: MongoClient;
  let db: Db, mddb: Db;
  let collectionName: string;
  let collection: Collection;
  beforeEach(async () => {
    collectionName = getRandomString();
    [client, db, mddb] = await openTestDB();
    await db.createCollection(collectionName);
    collection = db.collection(collectionName);
    await db.command({
      collMod: collectionName,
      changeStreamPreAndPostImages: { enabled: true },
    });
    runPersisterDetached().catch((e) =>
      // The cursor of course gets killed once the client gets closed
      // at the end of the test
      assert(e instanceof MongoServerError && e.codeName === "CursorKilled")
    );
    // Wait for the persister to start
    while (true) {
      const res = await mddb
        .collection<{ _id: string }>(MetadataCollectionName)
        .countDocuments({ _id: collectionName });
      if (res > 0) break;
      await sleep(500);
    }
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
      await sleep(50);
    }
  }

  describe("PCS invariants hold", () => {
    before(async () => {
      const [client, db] = await openTestDB();
      const pnis = z
        .object({ periodicNoopIntervalSecs: z.number() })
        .parse(
          await db
            .admin()
            .command({ getParameter: 1, periodicNoopIntervalSecs: 1 })
        ).periodicNoopIntervalSecs;
      assert.equal(
        pnis,
        10,
        "The tests assume a periodicNoopIntervalSecs value of 10 seconds"
      );
      await client.close();
    });
    it("for low activity", async () => {
      const pcsColl = mddb.collection(derivePCSCollName(collectionName));
      const pcscs = pcsColl.watch([]);
      // Buffer the persisted change stream events as they happen
      const buffer: PCSEventCommon[] = [];
      void (async () => {
        try {
          for await (const c of pcscs) {
            assert(c.operationType === "insert");
            buffer.push(zodPCSEventCommon.parse(c.fullDocument));
          }
        } catch (e) {
          // The change stream gets closed once the client gets closed
          // at the end of the test
          assert(
            e instanceof MongoServerError &&
              e.message === "ChangeStream is closed"
          );
        }
      })();
      // Wait for a noop
      while (buffer.findIndex((x) => x.o === "n") === -1) {
        await sleep(1000);
      }
      // Insert a document into the collection
      await collection.insertOne({});
      // Wait for two new noops
      let noopCntBefore = buffer.filter((x) => x.o === "n").length;
      while (buffer.filter((x) => x.o === "n").length < noopCntBefore + 2) {
        await sleep(1000);
      }
      // Insert two documents into the collection
      // Use a session to get the operation time
      let opTime!: Timestamp;
      await client.withSession(async (session) => {
        await collection.insertMany([{}, {}], { session });
        opTime = z.instanceof(Timestamp).parse(session.operationTime);
      });
      // Wait for the PCS to catch up
      while (
        buffer.length === 0 ||
        buffer
          .map((pcse) => pcse.ct)
          .reduce((a, b) => (a.gt(b) ? a : b))
          .lt(opTime)
      ) {
        await sleep(250);
      }
      // Wait for another nop
      noopCntBefore = buffer.filter((x) => x.o === "n").length;
      while (buffer.filter((x) => x.o === "n").length < noopCntBefore + 1) {
        await sleep(1000);
      }

      checkPCSInvariants(buffer);
    });
  });
});
