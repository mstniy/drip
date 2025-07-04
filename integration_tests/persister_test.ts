import {
  MongoClient,
  Db,
  MongoServerError,
  Collection,
  Timestamp,
  ObjectId,
} from "mongodb";
import { runPersister } from "../src/persister/persister";
import { openTestDB } from "../tests/test_utils/open_test_db";
import { getRandomString } from "../tests/test_utils/random_string";
import { strict as assert, AssertionError } from "assert";
import z from "zod";
import { MetadataCollectionName } from "../src/cea/metadata";
import { derivePCSCollName } from "../src/cea/derive_pcs_coll_name";
import { sleep } from "../tests/test_utils/sleep";
import { PCSEvent, zodPCSEvent } from "../src/cea/pcs_event";
import { describe, it, beforeEach, afterEach, before } from "node:test";
import { isBun } from "../tests/test_utils/is_bun";

function assertNonDescreasingCT(buffer: PCSEvent[]) {
  buffer
    .map((pcse) => pcse.ct)
    .reduce((a, b) => {
      assert(a.lte(b), "must be non-decreasing");
      return b;
    });
}

// This assumes the buffer is non-decreasing
function assertUniqueNoops(buffer: PCSEvent[]) {
  buffer
    // Only keep the noops
    .filter((x) => x.o === "n")
    .reduce((a, b) => {
      assert(b.ct.t > a.ct.t, "must be unique");
      return b;
    });
}

// This assumes the buffer is non-decreasing
function assertInfrequentNoops(buffer: PCSEvent[]) {
  buffer
    // Only keep the noops
    .filter((x) => x.o === "n")
    .reduce((a, b) => {
      assert(b.w.getTime() - a.w.getTime() > 5000, "must be infrequent");
      return b;
    });
}

function checkPCSInvariants(buffer: PCSEvent[]) {
  // Assert that the PCS events were created in
  // non-decreasing CT order
  assertNonDescreasingCT(buffer);

  // Assert that noops are unique
  // per cluster time t field
  assertUniqueNoops(buffer);

  // Assert that noops do not occur
  // too frequently
  assertInfrequentNoops(buffer);
}

describe("self test", () => {
  const reduceEmpty = `${isBun() ? "r" : "R"}educe of empty array with no initial value`;
  describe("assertNonDescreasingCT", () => {
    it("fails for an empty list", () => {
      try {
        assertNonDescreasingCT([]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(e instanceof TypeError && e.message === reduceEmpty);
      }
    });
    it("passes for a list with one element", () => {
      assertNonDescreasingCT([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
      ]);
    });
    it("passes for a list with two elements of the same CT", () => {
      assertNonDescreasingCT([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
      ]);
    });
    it("passes for a list with two elements of increasing CT", () => {
      assertNonDescreasingCT([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 1 }),
          o: "n",
          w: new Date(),
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
            w: new Date(),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date(),
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
        assert(e instanceof TypeError && e.message === reduceEmpty);
      }
    });
    it("fails for a list with no noops", () => {
      try {
        assertUniqueNoops([
          {
            _id: new ObjectId(),
            o: "d",
            ct: new Timestamp({ t: 0, i: 0 }),
            k: {},
            b: {},
          },
        ]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(e instanceof TypeError && e.message === reduceEmpty);
      }
    });
    it("passes for a list with one noop", () => {
      assertUniqueNoops([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
      ]);
    });
    it("passes for a list with nops on unique t-s", () => {
      assertUniqueNoops([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 1, i: 0 }),
          o: "n",
          w: new Date(),
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 2, i: 0 }),
          o: "n",
          w: new Date(),
        },
      ]);
    });
    it("fails for a list with nops on identical t-s", () => {
      const cases: PCSEvent[][] = [
        [
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date(),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date(),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 1, i: 0 }),
            o: "n",
            w: new Date(),
          },
        ],
        [
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date(),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 1, i: 0 }),
            o: "n",
            w: new Date(),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 1, i: 0 }),
            o: "n",
            w: new Date(),
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
  describe("assertInfrequentNoops", () => {
    it("fails for an empty list", () => {
      try {
        assertInfrequentNoops([]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(e instanceof TypeError && e.message === reduceEmpty);
      }
    });
    it("fails for a list with no noops", () => {
      try {
        assertInfrequentNoops([
          {
            _id: new ObjectId(),
            o: "d",
            ct: new Timestamp({ t: 0, i: 0 }),
            k: {},
            b: {},
          },
        ]);
        assert(false, "must have thrown");
      } catch (e) {
        assert(e instanceof TypeError && e.message === reduceEmpty);
      }
    });
    it("passes for a list with one noop", () => {
      assertInfrequentNoops([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date(),
        },
      ]);
    });
    it("passes for a list with well-spaced nops", () => {
      assertInfrequentNoops([
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date("2025-01-01"),
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date("2025-01-02"),
        },
        {
          _id: new ObjectId(),
          ct: new Timestamp({ t: 0, i: 0 }),
          o: "n",
          w: new Date("2025-01-03"),
        },
      ]);
    });
    it("fails for a list with too close noops", () => {
      const cases: PCSEvent[][] = [
        [
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date("2025-01-01"),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date("20205-01-02"),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date("20205-01-02"),
          },
        ],
        [
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date("20205-01-01"),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date("20205-01-01"),
          },
          {
            _id: new ObjectId(),
            ct: new Timestamp({ t: 0, i: 0 }),
            o: "n",
            w: new Date("20205-01-02"),
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
});

describe("persister", () => {
  let client: MongoClient;
  let db: Db, mddb: Db;
  let collectionName: string;
  let collection: Collection;
  let stopPersister: { stop: boolean };
  beforeEach(async () => {
    collectionName = getRandomString();
    [client, db, mddb] = await openTestDB();
    await db.createCollection(collectionName);
    collection = db.collection(collectionName);
    await db.command({
      collMod: collectionName,
      changeStreamPreAndPostImages: { enabled: true },
    });
    stopPersister = { stop: false };
    runPersisterDetached(stopPersister).catch((e) =>
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
  afterEach(async () => {
    stopPersister.stop = true;
    await client.close();
  });

  async function runPersisterDetached(stopPersister: { stop: boolean }) {
    const persister = runPersister(
      client,
      mddb.databaseName,
      db.collection(collectionName)
    );
    while (!stopPersister.stop) {
      await persister.next();
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
    it("for low activity", { timeout: 120000 }, async () => {
      const pcsColl = mddb.collection(derivePCSCollName(collectionName));
      const pcscs = pcsColl.watch([]);
      // Buffer the persisted change stream events as they happen
      const buffer: PCSEvent[] = [];
      void (async () => {
        try {
          for await (const c of pcscs) {
            assert(c.operationType === "insert");
            buffer.push(zodPCSEvent.parse(c.fullDocument));
          }
        } catch (e) {
          // The change stream gets closed once the client gets closed
          // at the end of the test
          assert(e instanceof MongoServerError);
        }
      })();
      // Wait for a noop
      while (buffer.findIndex((x) => x.o === "n") === -1) {
        await sleep(1000);
      }
      // Insert a document into the collection
      await collection.insertOne({});
      // Wait for two new noops
      const noopCntBefore = buffer.filter((x) => x.o === "n").length;
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
          .lte(opTime)
      ) {
        await sleep(250);
      }

      checkPCSInvariants(buffer);
    });
    it("for high activity", { timeout: 120000 }, async () => {
      const pcsColl = mddb.collection(derivePCSCollName(collectionName));
      const pcscs = pcsColl.watch([]);
      // Buffer the persisted change stream events as they happen
      const buffer: PCSEvent[] = [];
      void (async () => {
        try {
          for await (const c of pcscs) {
            assert(c.operationType === "insert");
            buffer.push(zodPCSEvent.parse(c.fullDocument));
          }
        } catch (e) {
          // The change stream gets closed once the client gets closed
          // at the end of the test
          assert(e instanceof MongoServerError);
        }
      })();
      // Insert a large number of documents into the collection
      // Use a session to get the operation time
      let opTime!: Timestamp;
      await client.withSession(async (session) => {
        await collection.insertMany(
          Array.from({ length: 50000 }).map((_) => {
            return {};
          }),
          { session }
        );
        opTime = z.instanceof(Timestamp).parse(session.operationTime);
      });
      // Wait for the PCS to catch up
      while (buffer.length === 0 || buffer[buffer.length - 1]!.ct.lte(opTime)) {
        await sleep(250);
      }

      checkPCSInvariants(buffer);
    });

    it("for small transactions", { timeout: 120000 }, async () => {
      const pcsColl = mddb.collection(derivePCSCollName(collectionName));
      const pcscs = pcsColl.watch([]);
      // Buffer the persisted change stream events as they happen
      const buffer: PCSEvent[] = [];
      void (async () => {
        try {
          for await (const c of pcscs) {
            assert(c.operationType === "insert");
            buffer.push(zodPCSEvent.parse(c.fullDocument));
          }
        } catch (e) {
          // The change stream gets closed once the client gets closed
          // at the end of the test
          assert(e instanceof MongoServerError);
        }
      })();
      // Insert a large number of documents into the collection transactionally
      let opTime!: Timestamp;
      await client.withSession(async (session) => {
        // This txn gets comitted as a single oplog entry
        await session.withTransaction(async (session) => {
          await collection.insertOne({}, { session });
          await collection.insertOne({}, { session });
        });
        opTime = z.instanceof(Timestamp).parse(session.operationTime);
      });
      // Wait for the PCS to catch up
      while (buffer.length === 0 || buffer[buffer.length - 1]!.ct.lte(opTime)) {
        await sleep(250);
      }

      checkPCSInvariants(buffer);

      // All the persisted non-noop events must have the same cluster time
      for (const pcse of buffer.filter((o) => o.o != "n")) {
        assert(pcse.ct.eq(opTime));
      }
    });

    it("for large transactions", { timeout: 120000 }, async () => {
      const pcsColl = mddb.collection(derivePCSCollName(collectionName));
      const pcscs = pcsColl.watch([]);
      // Buffer the persisted change stream events as they happen
      const buffer: PCSEvent[] = [];
      void (async () => {
        try {
          for await (const c of pcscs) {
            assert(c.operationType === "insert");
            buffer.push(zodPCSEvent.parse(c.fullDocument));
          }
        } catch (e) {
          // The change stream gets closed once the client gets closed
          // at the end of the test
          assert(e instanceof MongoServerError);
        }
      })();
      // Insert a large number of documents into the collection transactionally
      let opTime!: Timestamp;
      await client.withSession(async (session) => {
        await session.withTransaction(async (session) => {
          // This txn is so large it needs to be divided up
          // into ~10 oplog entries.
          await collection.insertMany(
            Array.from({ length: 10000 }).map((_) => {
              return {};
            }),
            { session }
          );
        });
        opTime = z.instanceof(Timestamp).parse(session.operationTime);
      });
      // Wait for the PCS to catch up
      while (buffer.length === 0 || buffer[buffer.length - 1]!.ct.lte(opTime)) {
        await sleep(250);
      }

      checkPCSInvariants(buffer);

      // All the persisted non-noop events must have the same cluster time
      for (const pcse of buffer.filter((o) => o.o != "n")) {
        assert(pcse.ct.eq(opTime));
      }
    });
  });
});
