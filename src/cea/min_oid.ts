import { ObjectId } from "mongodb";

// Note that this technically is a valid OID, and CEA does gt queries.
export const minOID = ObjectId.createFromHexString("000000000000000000000000");
