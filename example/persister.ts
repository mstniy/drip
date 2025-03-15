import { MongoClient } from "mongodb";
import { startPersister } from "../src";
import { collName, dbName, mongoURL } from "./constants";

async function main() {
  const client = new MongoClient(mongoURL);
  const db = client.db(dbName);
  const coll = db.collection(collName);

  console.log("Starting the Drip persister...");

  await startPersister(coll, db);
}

void main();
