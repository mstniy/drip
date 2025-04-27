import { strict as assert } from "assert";
import { describe, it } from "../test_utils/tests_polyglot";
import { ObjectId } from "mongodb";
import { oidLT } from "../../src/cea/oid_less";

describe("oidLT", () => {
  const pairs = [
    [
      ObjectId.createFromHexString("67bce7f5826f2020ba51e944"),
      ObjectId.createFromHexString("67bce7f7826f2020ba51e945"),
    ],
    [
      ObjectId.createFromHexString("66bce7f5826f2020ba51e945"),
      ObjectId.createFromHexString("67bce7f5826f2020ba51e944"),
    ],
  ] as const;
  it("returns true if a<b", () => {
    for (const pair of pairs.values()) {
      assert.equal(oidLT(pair[0], pair[1]), true);
    }
  });
  it("returns false if a>b", () => {
    for (const pair of pairs.values()) {
      assert.equal(oidLT(pair[1], pair[0]), false);
    }
  });
  it("returns false if a==b", () => {
    for (const pair of pairs.values()) {
      assert.equal(oidLT(pair[0], pair[0]), false);
    }
  });
});
