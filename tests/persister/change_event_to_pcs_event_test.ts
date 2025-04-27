import { describe, it } from "bun:test";
import { ObjectId, Timestamp, UUID } from "mongodb";
import { strict as assert } from "assert";
import {
  zodPCSInsertionEvent,
  PCSInsertionEvent,
  zodPCSUpdateEvent,
  PCSUpdateEvent,
  zodPCSDeletionEvent,
  PCSDeletionEvent,
} from "../../src/cea/pcs_event";
import { changeEventToPCSEvent } from "../../src/persister/change_event_to_pcs_event";

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
    } satisfies PCSInsertionEvent);
  });
  it("can convert replace events", () => {
    const id = new ObjectId();
    const cse = {
      operationType: "replace",
      _id: null,
      collectionUUID: new UUID(),
      documentKey: { _id: id, foo: "bar" },
      fullDocument: { _id: id, foo: "bar", b: 0 },
      fullDocumentBeforeChange: { _id: id, foo: "bar", a: 0 },
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
      a: cse.fullDocument,
      b: cse.fullDocumentBeforeChange,
      ct: cse.clusterTime,
      k: cse.documentKey,
      o: "u",
    } satisfies PCSUpdateEvent);
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
