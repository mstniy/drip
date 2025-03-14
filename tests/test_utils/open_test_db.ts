import { MongoClient } from "mongodb";

const TEST_DB_NAME = "drip_test";

export async function openTestDB() {
  const client = new MongoClient("mongodb://127.0.0.1:27017");
  await client.connect();
  const db = client.db(TEST_DB_NAME);

  return [client, db] as const;
}
