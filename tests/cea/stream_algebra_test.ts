import { strict as assert } from "assert";
import { describe, it } from "node:test";
import { streamAppend, streamSquashMerge, streamTake } from "../../src/cea/stream_algebra";
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

describe("streamAppend", () => {
  it("can append zero streams", async () => {
    assert.deepStrictEqual(await genToArray(streamAppend([])), []);
  });
  it("can append one stream", async () => {
    assert.deepStrictEqual(await genToArray(streamAppend([streamFrom([0])])), [0]);
  });
  it("can append two streams", async () => {
    assert.deepStrictEqual(await genToArray(streamAppend([streamFrom([0]), streamFrom([1])])), [0, 1]);
  });
  it("is lazy", async () => {
    let flag1 = 0;
    let flag2 = 0;

    // eslint-disable-next-line @typescript-eslint/require-await
    const res = streamAppend([async function* () { flag1++; yield 0; }(), async function* () { flag2++; yield 1; }()]);

    assert.deepStrictEqual([flag1, flag2], [0, 0]);
    assert.equal((await res.next()).value, 0);
    assert.deepStrictEqual([flag1, flag2], [1, 0]);
    assert.equal((await res.next()).value, 1);
    assert.deepStrictEqual([flag1, flag2], [1, 1]);
    assert((await res.next()).done);
  });
});

describe('streamTake', () => {
  it('can take zero elements', async () => {
    // eslint-disable-next-line require-yield, @typescript-eslint/require-await
    assert.deepStrictEqual(await genToArray(streamTake(0, (async function* () { assert(false, 'must not be called'); })())), []);
  });
  it('can take two elements', async () => {
    assert.deepStrictEqual(await genToArray(streamTake(2, streamFrom([1, 2, 3]))), [1, 2]);
  });
  it('can overrun the source stream', async () => {
    assert.deepStrictEqual(await genToArray(streamTake(2, streamFrom([1]))), [1]);
  });
  it('is lazy', async () => {
    // eslint-disable-next-line @typescript-eslint/require-await
    assert.deepStrictEqual(await genToArray(streamTake(2, (async function* () { yield 1; yield 2; assert(false, 'must not be called'); })())), [1, 2]);
  });
});