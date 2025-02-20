// Some Node drivers have means to compare object ids
// eg. https://github.com/mongodb/mongo-csharp-driver/blob/bfc43d64016048d634f0f21d552df6339626ca87/src/MongoDB.Bson/ObjectModel/ObjectId.cs#L359
// But not the JS BSON implementation. So we roll our own.

import { strict as assert } from "assert";
import { ObjectId } from "mongodb";

// Return true iff the first object id is less than the second.
export function oidLT(a: ObjectId, b: ObjectId): boolean {
  const BUF_LEN = 12;

  const arr1 = a.id;
  const arr2 = b.id;
  assert(arr1.length === BUF_LEN);
  assert(arr2.length === BUF_LEN);
  for (let i = 0; i < BUF_LEN; i++) {
    if (arr1[i] !== arr2[i]) {
      return arr1[i]! < arr2[i]!;
    }
  }

  return false;
}
