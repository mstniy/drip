import { MongoClient, ObjectId } from "mongodb";
import {
  CEACursor,
  CSEvent,
  dripCC,
  dripCEAResume,
  dripCEAStart,
  applyUpdateDescription,
} from "../src";
import z from "zod";
import { collName, dbName, mongoURL } from "./constants";
import { strict as assert } from "assert";

async function genToArray<T, R>(
  gen: AsyncGenerator<T, R, void>
): Promise<[T[], R]> {
  const arr: T[] = [];

  while (true) {
    const res = await gen.next();
    if (!res.done) {
      arr.push(res.value);
    } else {
      return [arr, res.value];
    }
  }
}

const zodTodo = z.object({
  title: z.string(),
});

const zodTodoWithId = zodTodo.merge(
  z.object({
    _id: z.instanceof(ObjectId),
  })
);

async function* sync() {
  // Note that we don't do persistence for this demo,
  // but a real application would likely want to do it.

  const pipeline = [{ $match: { userId: "me" } }];

  // 5 seconds of buffer
  const syncStart = new Date(Date.now() - 5 * 1000);

  const client = new MongoClient(mongoURL);
  const db = client.db(dbName);

  console.log("Starting collection copy...");

  const ccRes = await genToArray(dripCC(db, collName, undefined, pipeline));

  const ccEnd = ccRes[1];

  const subset = Object.fromEntries(
    ccRes[0]
      .map((d) => zodTodoWithId.parse(d))
      .map((d) => [d._id.toHexString(), zodTodo.parse(d)] as const)
  );

  let ceaCursor: CEACursor | undefined;

  function handleChange(c: CSEvent) {
    ceaCursor = c.cursor;
    switch (c.operationType) {
      case "addition": {
        const todo = zodTodoWithId.parse(c.fullDocument);
        subset[todo._id.toHexString()] = zodTodo.parse(todo);
        break;
      }
      case "subtraction": {
        delete subset[z.instanceof(ObjectId).parse(c.id).toHexString()];
        break;
      }
      case "update": {
        const update = c.updateDescription;
        const id = z.instanceof(ObjectId).parse(c.id);
        const old = subset[id.toHexString()];
        assert(old);
        subset[id.toHexString()] = zodTodo.parse(
          applyUpdateDescription(old, update)
        );
        break;
      }
      case "noop":
        // Nothing to do
        break;
      default:
        // No unhandled case
        c satisfies never;
        break;
    }
  }

  console.log("Starting change event application...");

  let yielded = false;

  while (true) {
    let gotMeaningfulChange = false;
    for await (const c of typeof ceaCursor === "undefined"
      ? dripCEAStart(db, collName, syncStart, pipeline)
      : dripCEAResume(db, collName, ceaCursor, pipeline)) {
      if (c.operationType !== "noop") {
        gotMeaningfulChange = true;
      }
      handleChange(c);
    }
    const isConsistent = ceaCursor && ceaCursor.clusterTime.gte(ccEnd);
    if (isConsistent && (gotMeaningfulChange || !yielded)) {
      yielded = true;
      yield subset;
    }
    await new Promise((res) => setTimeout(res, 1000));
  }
}

async function main() {
  for await (const snapshot of sync()) {
    console.log(snapshot);
  }
}

void main();
