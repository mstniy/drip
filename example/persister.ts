import { MongoClient } from "mongodb";
import { collName, dbName, mongoURL } from "./constants";
import { runPersister } from "../src/persister/persister";

async function main() {
  const client = new MongoClient(mongoURL);
  const db = client.db(dbName);
  const coll = db.collection(collName);

  console.log("Starting the Drip persister...");

  const persister = runPersister(coll, db);

  while (true) {
    await persister.next();
  }
}

void main();
