import { MongoClient } from "mongodb";
import { collName, dbName, mongoURL } from "./constants";
import { runPersister } from "../src/persister/persister";

async function main() {
  const client = new MongoClient(mongoURL);
  const db = client.db(dbName);
  const coll = db.collection(collName);

  console.log("Starting the Drip persister...");

  const persister = runPersister(client, dbName, coll, {
    // Use a small max await time to reduce latency
    // Note that this likely would reduce throughput,
    // so a production system would likely use a larger
    // value, or the default.
    maxAwaitTimeMS: 1000,
  });

  while (true) {
    await persister.next();
  }
}

void main();
