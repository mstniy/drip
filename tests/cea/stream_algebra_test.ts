import { strict as assert } from "assert";
import { describe, it } from "node:test";
import { streamSquashMerge } from "../../src/cea/stream_algebra";
import { genToArray } from "../test_utils/gen_to_array";

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
