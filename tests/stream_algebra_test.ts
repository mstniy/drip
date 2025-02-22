import { strict as assert } from "assert";
import { describe, it } from "node:test";
import { genToArray } from "./gen_to_array";
import { streamAdd, streamSubtract } from "../src/cea/stream_algebra";

async function* streamFrom<T>(x: T[], cleanup?: () => void) {
  try {
    yield* x;
  } finally {
    if (cleanup) {
      cleanup();
    }
  }
}

describe("streamAdd", () => {
  it("can add two empty streams", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamAdd(
          streamFrom([]),
          streamFrom([]),
          (a: number, b: number) => a < b
        )
      ),
      []
    );
  });
  it("LHS can be empty", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamAdd(
          streamFrom([]),
          streamFrom([1]),
          (a: number, b: number) => a < b
        )
      ),
      [1]
    );
  });
  it("RHS can be empty", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamAdd(
          streamFrom([1]),
          streamFrom([]),
          (a: number, b: number) => a < b
        )
      ),
      [1]
    );
  });
  it("does addition", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamAdd(
          streamFrom([0, 2, 3, 5]),
          streamFrom([0, 1, 4]),
          (a: number, b: number) => a < b
        )
      ),
      [0, 0, 1, 2, 3, 4, 5]
    );
  });
});

describe("streamSubtract", () => {
  it("can subtract two empty streams", async () => {
    let done = [false, false];
    assert.deepStrictEqual(
      await genToArray(
        streamSubtract(
          streamFrom([], () => (done[0] = true)),
          streamFrom([], () => (done[1] = true)),
          (a: number, b: number) => a < b
        )
      ),
      []
    );
    assert.deepStrictEqual(done, [true, true]);
  });
  it("LHS can be empty", async () => {
    let done = [false, false];
    assert.deepStrictEqual(
      await genToArray(
        streamSubtract(
          streamFrom([], () => (done[0] = true)),
          streamFrom([1], () => (done[1] = true)),
          (a: number, b: number) => a < b
        )
      ),
      []
    );
    assert.deepStrictEqual(done, [true, true]);
  });
  it("RHS can be empty", async () => {
    let done = [false, false];
    assert.deepStrictEqual(
      await genToArray(
        streamSubtract(
          streamFrom([1], () => (done[0] = true)),
          streamFrom([], () => (done[1] = true)),
          (a: number, b: number) => a < b
        )
      ),
      [1]
    );
    assert.deepStrictEqual(done, [true, true]);
  });
  it("does subtraction", async () => {
    let done = [false, false];
    assert.deepStrictEqual(
      await genToArray(
        streamSubtract(
          streamFrom([0, 2, 3, 4, 6], () => (done[0] = true)),
          streamFrom([0, 1, 3, 5, 6], () => (done[1] = true)),
          (a: number, b: number) => a < b
        )
      ),
      [2, 4]
    );
    assert.deepStrictEqual(done, [true, true]);
  });
});
