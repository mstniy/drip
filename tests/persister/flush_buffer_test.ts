import { it } from "node:test";
import { FlushBuffer } from "../../src/persister/flush_buffer";
import { strict as assert } from "assert";

it("1", async () => {
  let flushedItems: number[] | undefined;
  // eslint-disable-next-line @typescript-eslint/require-await
  const buf = new FlushBuffer<number>(2, async (items) => {
    flushedItems = [...items];
  });

  await buf.push(0);
  assert.equal(flushedItems, undefined);
  await buf.push(1);
  assert.deepStrictEqual(flushedItems, [0, 1]);

  // Must also reset the state
  flushedItems = undefined;
  await buf.push(2);
  assert.equal(flushedItems, undefined);
  await buf.push(3);
  assert.deepStrictEqual(flushedItems, [2, 3]);

  // Must not flush again
  flushedItems = undefined;
  await new Promise((res) => setTimeout(res, 0));
  await new Promise((res) => setTimeout(res, 0));
  assert.equal(flushedItems, undefined);

  // Push another item
  await buf.push(4);
  assert.equal(flushedItems, undefined);

  // Wait for a task -> must push
  await new Promise((res) => setTimeout(res, 0));
  assert.deepStrictEqual(flushedItems, [4]);
});

it("2", async () => {
  let flushedItems: number[] | undefined;
  // eslint-disable-next-line @typescript-eslint/require-await
  const buf = new FlushBuffer<number>(2, async (items) => {
    flushedItems = [...items];
  });

  await buf.push(0);
  assert.equal(flushedItems, undefined);

  // Wait for the next task, letting that of the
  // buffer fire.
  await new Promise((res) => setTimeout(res, 0));

  // Must have flushed the item
  assert.deepStrictEqual(flushedItems, [0]);

  // Must also reset the state
  // Does not flush again
  flushedItems = undefined;
  await new Promise((res) => setTimeout(res, 0));
  await new Promise((res) => setTimeout(res, 0));
  assert.equal(flushedItems, undefined);

  // Takes two new items to flush
  await buf.push(1);
  assert.equal(flushedItems, undefined);
  await buf.push(2);
  assert.deepStrictEqual(flushedItems, [1, 2]);
});
