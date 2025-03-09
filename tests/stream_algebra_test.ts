import { strict as assert } from "assert";
import { describe, it } from "node:test";
import { genToArray } from "./gen_to_array";
import { streamAdd, streamSquashMerge } from "../src/cea/stream_algebra";

async function* streamFrom<T>(x: T[], cleanup?: () => void) {
  try {
    for (const xx of x) {
      yield await Promise.resolve(xx);
    }
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

describe("streamSquashMerge", () => {
  it("can have no streams", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamSquashMerge([], () => {
          throw new Error("must not be called");
        })
      ),
      []
    );
  });
  it("can have one stream", async () => {
    assert.deepStrictEqual(
      await genToArray(streamSquashMerge([streamFrom([1])], (a, b) => a < b)),
      [1]
    );
  });
  it("does squash merge", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamSquashMerge(
          [
            streamFrom([0, 3]),
            streamFrom([0, 1, 3, 4]),
            streamFrom([0, 2, 3, 5]),
          ],
          (a, b) => a < b
        )
      ),
      [0, 1, 2, 3, 4, 5]
    );
  });
  it("yields items from earlier streams in case of equality", async () => {
    const a1 = {},
      a2 = {};
    const res = await genToArray(
      streamSquashMerge([streamFrom([a1]), streamFrom([a2])], (_a, _b) => false)
    );
    assert.equal(res.length, 1);
    assert(res[0] === a1); // and not a2
  });
});
