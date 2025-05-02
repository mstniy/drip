import { strict as assert } from "assert";
import { describe, it } from "node:test";
import {
  applyUpdateDescription,
  InvalidUpdateDescription,
} from "../../src/cea/update_description";

describe("applyUpdateDescription", () => {
  describe("field upserts", () => {
    it("work", () => {
      const old = Object.freeze({ b: false });
      assert.deepStrictEqual(applyUpdateDescription(old, { i: { a: true } }), {
        a: true,
        b: false,
      });
    });
    it("are idempotent", () => {
      const old = Object.freeze({ b: false, a: true });
      assert.deepStrictEqual(applyUpdateDescription(old, { i: { a: true } }), {
        a: true,
        b: false,
      });
    });
  });
  describe("field deletions", () => {
    it("work", () => {
      const old = Object.freeze({ a: true, b: false });
      assert.deepStrictEqual(applyUpdateDescription(old, { d: { a: false } }), {
        b: false,
      });
    });
    it("are idempotent", () => {
      const old = Object.freeze({ b: false });
      assert.deepStrictEqual(applyUpdateDescription(old, { d: { a: false } }), {
        b: false,
      });
    });
  });
  describe("array truncations", () => {
    it("can apply", () => {
      const old = Object.freeze({ a: Object.freeze([1, 2, 3]), b: 0 });
      assert.deepStrictEqual(applyUpdateDescription(old, { t: { a: 1 } }), {
        a: [1],
        b: 0,
      });
    });
    it("the field must be an array", () => {
      try {
        const old = Object.freeze({ a: 0 });
        applyUpdateDescription(old, { t: { a: 1 } });
        throw new Error("must have thrown");
      } catch (e) {
        assert(
          e instanceof InvalidUpdateDescription &&
            e.message === "Expected array, got number"
        );
      }
    });
    it("the key must be a number", () => {
      try {
        const old = Object.freeze({ a: Object.freeze([1]) });
        applyUpdateDescription(old, { t: { a: "test" } });
        throw new Error("must have thrown");
      } catch (e) {
        assert(
          e instanceof InvalidUpdateDescription &&
            e.message === "Expected number, got string"
        );
      }
    });
  });
  describe("nested updates", () => {
    it("can apply", () => {
      const old = Object.freeze({ a: Object.freeze({}) });
      assert.deepStrictEqual(
        applyUpdateDescription(old, { sa: { i: { b: 0 } } }),
        { a: { b: 0 } }
      );
    });
    it("the field must be an object", () => {
      try {
        const old = Object.freeze({});
        applyUpdateDescription(old, { sa: { i: { b: 0 } } });
        throw new Error("must have thrown");
      } catch (e) {
        assert(
          e instanceof InvalidUpdateDescription &&
            e.message === "Expected nested object, got undefined"
        );
      }
    });
  });
  it("returns as-is if there are no updates", () => {
    const old = Object.freeze({ a: 0 });
    assert.deepStrictEqual(applyUpdateDescription(old, {}), { a: 0 });
  });
  it("values must be objects", () => {
    try {
      const old = Object.freeze({});
      applyUpdateDescription(old, { i: 0 });
      throw new Error("must have thrown");
    } catch (e) {
      assert(
        e instanceof InvalidUpdateDescription &&
          e.message === "Expected object, got number"
      );
    }
  });
  it("throws for unknown keys", () => {
    try {
      const old = Object.freeze({});
      applyUpdateDescription(old, { test: {} });
      throw new Error("must have thrown");
    } catch (e) {
      assert(
        e instanceof InvalidUpdateDescription &&
          e.message === 'Unexpected key: "test"'
      );
    }
  });
  it("can do all at once", () => {
    const old = Object.freeze({ a: [1, 2, 3], b: {}, c: 0 });
    assert.deepStrictEqual(
      applyUpdateDescription(old, {
        d: { c: false },
        i: { d: 1 },
        t: { a: 2 },
        sb: { i: { a: 0 } },
      }),
      {
        a: [1, 2],
        b: { a: 0 },
        d: 1,
      }
    );
  });
});
