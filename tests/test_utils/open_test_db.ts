import { MongoClient } from "mongodb";

const TEST_DB_NAME = "drip_test";
const TEST_MD_DB_NAME = "drip_test_metadata";

export async function openTestDB() {
  const client = new MongoClient("mongodb://127.0.0.1:27017");
  await client.connect();
  const db = client.db(TEST_DB_NAME);
  const mddb = client.db(TEST_MD_DB_NAME);

  return [client, db, mddb] as const;
}
