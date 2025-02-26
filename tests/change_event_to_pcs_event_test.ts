import { describe, it } from "node:test";
import { changeEventToPCSEvent } from "../src/persister/change_event_to_pcs_event";
import { ObjectId, Timestamp, UUID } from "mongodb";
import {
  PCSDeletionEvent,
  PCSInsertionEvent,
  PCSUpdateEvent,
  zodPCSDeletionEvent,
  zodPCSInsertionEvent,
  zodPCSUpdateEvent,
} from "../src/cea/pcs_event";
import { strict as assert } from "assert";

describe("changeEventToPCSEvent", () => {
  it("can convert insertion events", () => {
    const id = new ObjectId();
    const cse = {
      operationType: "insert",
      _id: null,
      collectionUUID: new UUID(),
      documentKey: { _id: id, foo: "bar" },
      fullDocument: { _id: id, foo: "bar", a: 0 },
      ns: {
        coll: "",
        db: "",
      },
      clusterTime: new Timestamp({ t: 5, i: 10 }),
      wallTime: new Date(),
    } as const;
    const res = zodPCSInsertionEvent.parse(changeEventToPCSEvent(cse));
    assert.deepStrictEqual(res, {
      _id: res._id,
      a: cse.fullDocument,
      ct: cse.clusterTime,
      k: cse.documentKey,
      o: "i",
      v: 1,
      w: cse.wallTime,
    } satisfies PCSInsertionEvent);
  });
  it("can convert update events", () => {
    const id = new ObjectId();
    const cse = {
      operationType: "update",
      _id: null,
      collectionUUID: new UUID(),
      documentKey: { _id: id, foo: "bar" },
      fullDocument: { _id: id, foo: "bar", a: 0 },
      fullDocumentBeforeChange: { _id: id, foo: "bar", a: 1 },
      updateDescription: {
        updatedFields: {
          a: 0,
        },
      },
      ns: {
        coll: "",
        db: "",
      },
      clusterTime: new Timestamp({ t: 5, i: 10 }),
      wallTime: new Date(),
    } as const;
    const res = zodPCSUpdateEvent.parse(changeEventToPCSEvent(cse));
    assert.deepStrictEqual(res, {
      _id: res._id,
      b: cse.fullDocumentBeforeChange,
      a: cse.fullDocument,
      u: {
        i: {
          a: 0,
        },
      },
      ct: cse.clusterTime,
      k: cse.documentKey,
      o: "u",
      v: 1,
      w: cse.wallTime,
    } satisfies PCSUpdateEvent);
  });
  it("can convert deletion events", () => {
    const id = new ObjectId();
    const cse = {
      operationType: "delete",
      _id: null,
      collectionUUID: new UUID(),
      documentKey: { _id: id, foo: "bar" },
      fullDocumentBeforeChange: { _id: id, foo: "bar", a: 0 },
      ns: {
        coll: "",
        db: "",
      },
      clusterTime: new Timestamp({ t: 5, i: 10 }),
      wallTime: new Date(),
    } as const;
    const res = zodPCSDeletionEvent.parse(changeEventToPCSEvent(cse));
    assert.deepStrictEqual(res, {
      _id: res._id,
      b: cse.fullDocumentBeforeChange,
      ct: cse.clusterTime,
      k: cse.documentKey,
      o: "d",
      v: 1,
      w: cse.wallTime,
    } satisfies PCSDeletionEvent);
  });
  it("ignores unknown event types", () => {
    const res = changeEventToPCSEvent({
      operationType: "createIndexes",
      _id: new ObjectId(),
      collectionUUID: new UUID(),
    });
    assert.equal(res, undefined);
  });
});
