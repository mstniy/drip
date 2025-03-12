import { MongoClient, ObjectId } from "mongodb";
import {
  CEACursor,
  CSEvent,
  dripCC,
  dripCEAResume,
  dripCEAStart,
} from "../src/drip";
import z from "zod";
import { strict as assert } from "assert";
import { collName, dbName, mongoURL } from "./constants";

async function genToArray<T>(gen: AsyncGenerator<T, void, void>): Promise<T[]> {
  const res: T[] = [];
  for await (const t of gen) {
    res.push(t);
  }
  return res;
}

const zodTodo = z.object({
  _id: z.instanceof(ObjectId),
  deleted: z.boolean(),
  title: z.string(),
});

async function* sync() {
  // Note that we don't do persistence for this demo,
  // but a real application would likely want to do it.

  const rule = { stages: [{ $match: { deleted: false } }] };

  // 15 mins of buffer
  const syncStart = new Date(Date.now() - 15 * 60 * 1000);

  const client = new MongoClient(mongoURL);
  const db = client.db(dbName);

  console.log("Starting collection copy...");

  const subset = (await genToArray(dripCC(db, collName, rule))).map((d) =>
    zodTodo.parse(d)
  );

  let ceaCursor: CEACursor | undefined;

  function handleChange(c: CSEvent) {
    ceaCursor = c.cursor;
    switch (c.operationType) {
      case "addition": {
        const todo = zodTodo.parse(c.fullDocument);
        const idx = subset.findIndex((t) => t._id.equals(todo._id));
        if (idx === -1) {
          subset.push(todo);
        } else {
          // Note that this might happen because
          // eg. we have already received this
          // document during collection copy
          subset[idx] = zodTodo.parse(todo);
        }
        break;
      }
      case "subtraction": {
        const idx = subset.findIndex((t) =>
          t._id.equals(z.instanceof(ObjectId).parse(c.id))
        );
        assert(idx !== -1);
        subset.splice(idx, 1);
        break;
      }
      case "update": {
        const idx = subset.findIndex((t) =>
          t._id.equals(z.instanceof(ObjectId).parse(c.id))
        );
        assert(idx !== -1);
        //const update = c.updateDescription;
        //TODO: Apply the update
        break;
      }
      default:
        // No unhandled case
        c satisfies never;
        break;
    }
  }

  console.log("Starting change event application...");

  let yielded = false;

  while (true) {
    let gotChange = false;
    for await (const c of typeof ceaCursor === "undefined"
      ? dripCEAStart(db, collName, syncStart, rule)
      : dripCEAResume(db, ceaCursor, rule)) {
      gotChange = true;
      handleChange(c);
    }
    if (gotChange || !yielded) {
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
