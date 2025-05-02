import { strict as assert } from "assert";
import { describe, it } from "node:test";
import { isComposite } from "../../../src/cea/scope_ppl/atoms";
import { Long } from "mongodb";

describe("isComposite", () => {
  it("number is not composite", () => {
    assert.equal(isComposite(0), false);
  });
  it("Date is not composite", () => {
    assert.equal(isComposite(new Date()), false);
  });
  it("regex is not composite", () => {
    assert.equal(isComposite(/hey/), false);
  });
  it("Long is not composite", () => {
    assert.equal(isComposite(new Long(0, 0)), false);
  });
  it("array is composite", () => {
    assert.equal(isComposite([]), true);
  });
  it("records are composite", () => {
    assert.equal(isComposite({}), true);
  });
});
