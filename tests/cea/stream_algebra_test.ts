import { strict as assert } from "assert";
import { describe, it } from "node:test";
import {
  streamAppend,
  streamSquashMerge,
  streamTake,
} from "../../src/cea/stream_algebra";
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
  it("closes the underlying streams", async () => {
    let cleaned = false;
    const ssm = streamSquashMerge(
      [
        // eslint-disable-next-line @typescript-eslint/require-await
        (async function* () {
          try {
            yield 0;
            yield 1;
          } finally {
            cleaned = true;
          }
        })(),
      ],
      (_a, _b) => {
        throw new Error("Must never be called");
      }
    );

    assert.equal((await ssm.next()).value, 0);
    assert.equal(cleaned, false);
    await ssm.return();
    assert.equal(cleaned, true);
  });
});

describe("streamAppend", () => {
  it("can append zero streams", async () => {
    assert.deepStrictEqual(await genToArray(streamAppend([])), []);
  });
  it("can append one stream", async () => {
    assert.deepStrictEqual(await genToArray(streamAppend([streamFrom([0])])), [
      0,
    ]);
  });
  it("can append two streams", async () => {
    assert.deepStrictEqual(
      await genToArray(streamAppend([streamFrom([0]), streamFrom([1])])),
      [0, 1]
    );
  });
  it("is lazy", async () => {
    let flag1 = 0;
    let flag2 = 0;

    const res = streamAppend([
      // eslint-disable-next-line @typescript-eslint/require-await
      (async function* () {
        flag1++;
        yield 0;
      })(),
      // eslint-disable-next-line @typescript-eslint/require-await
      (async function* () {
        flag2++;
        yield 1;
      })(),
    ]);

    assert.deepStrictEqual([flag1, flag2], [0, 0]);
    assert.equal((await res.next()).value, 0);
    assert.deepStrictEqual([flag1, flag2], [1, 0]);
    assert.equal((await res.next()).value, 1);
    assert.deepStrictEqual([flag1, flag2], [1, 1]);
    assert((await res.next()).done);
  });
  it("closes the underlying streams", async () => {
    let cleaned = false;
    const s = // eslint-disable-next-line @typescript-eslint/require-await
      (async function* () {
        try {
          yield 0;
          yield 1;
        } finally {
          cleaned = true;
        }
      })();

    assert.equal((await s.next()).value, 0);

    const ssm = streamAppend([streamFrom([1, 2]), s]);

    assert.equal((await ssm.next()).value, 1);
    assert.equal(cleaned, false);
    await ssm.return();
    assert.equal(cleaned, true);
  });
});

describe("streamTake", () => {
  it("can take zero elements", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamTake(
          0,
          // eslint-disable-next-line require-yield, @typescript-eslint/require-await
          (async function* () {
            assert(false, "must not be called");
          })()
        )
      ),
      []
    );
  });
  it("can take two elements", async () => {
    assert.deepStrictEqual(
      await genToArray(streamTake(2, streamFrom([1, 2, 3]))),
      [1, 2]
    );
  });
  it("can overrun the source stream", async () => {
    assert.deepStrictEqual(await genToArray(streamTake(2, streamFrom([1]))), [
      1,
    ]);
  });
  it("is lazy", async () => {
    assert.deepStrictEqual(
      await genToArray(
        streamTake(
          2,
          // eslint-disable-next-line @typescript-eslint/require-await
          (async function* () {
            yield 1;
            yield 2;
            assert(false, "must not be called");
          })()
        )
      ),
      [1, 2]
    );
  });
  it("closes the underlying stream", async () => {
    let cleaned = false;
    const s = // eslint-disable-next-line @typescript-eslint/require-await
      (async function* () {
        try {
          yield 0;
          yield 1;
          yield 2;
        } finally {
          cleaned = true;
        }
      })();

    assert.equal((await s.next()).value, 0);

    const ssm = streamTake(0, s);

    assert.equal(cleaned, false);
    assert.equal((await ssm.next()).done, true);
    assert.equal(cleaned, true);
  });
});
