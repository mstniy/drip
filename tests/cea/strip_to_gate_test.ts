import { strict as assert } from "assert";
import { stripToGate } from "../../src/cea/strip_to_gate";
import { describe, it } from "../test_utils/tests_polyglot";

describe("stripToGate", () => {
  it("works for empty pipelines", () => {
    assert.deepStrictEqual(stripToGate([]), []);
  });
  it("works for pipelines with no match stages", () => {
    assert.deepStrictEqual(
      stripToGate([{ type: "addFields", fields: { a: 0 } }]),
      []
    );
  });
  it("works for pipelines with match stages", () => {
    assert.deepStrictEqual(
      stripToGate([
        { type: "addFields", fields: { a: 0 } },
        { type: "match", filter: { a: 0 } },
        { type: "addFields", fields: { b: 0 } },
        { type: "match", filter: { b: 0 } },
        { type: "addFields", fields: { c: 0 } },
      ]),
      [
        { type: "addFields", fields: { a: 0 } },
        { type: "match", filter: { a: 0 } },
        { type: "addFields", fields: { b: 0 } },
        { type: "match", filter: { b: 0 } },
      ]
    );
  });
});
