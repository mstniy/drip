import { it } from "node:test";
import { PromiseTrain } from "../../src/persister/promise_train";
import { strict as assert } from "assert";
import { sleep } from "../test_utils/sleep";

it("subsequent pushes await the previous ones", async () => {
  const train = new PromiseTrain();

  let p1res!: (r: unknown) => void;
  const p1 = new Promise((res) => (p1res = res));

  // The push must return right away, as there are no
  // other operations queued.
  await train.push(() => p1);

  let p2res!: (r: unknown) => void;
  let p2flag = false;
  let p2ack = false;
  const p2 = new Promise((res) => (p2res = res));

  void train
    .push(() => {
      p2flag = true;
      return p2;
    })
    .then(() => (p2ack = true));

  let p3res!: (r: unknown) => void;
  let p3flag = false;
  let p3ack = false;
  const p3 = new Promise((res) => (p3res = res));

  void train
    .push(() => {
      p3flag = true;
      return p3;
    })
    .then(() => (p3ack = true));

  // Wait for a task because why not
  await sleep(0);

  // Operations in subsequent pushes have not started yet
  // They are waiting for p1
  assert.equal(p2flag, false);
  assert.equal(p3flag, false);

  // The calls to push are waiting as well
  assert.equal(p2ack, false);
  assert.equal(p3ack, false);

  // p1 finishes
  p1res(null);

  // Wait for a task because why not
  await sleep(0);

  // p2 has started
  assert.equal(p2flag, true);
  // p3 is waiting for p2
  assert.equal(p3flag, false);

  // The calls to push p2 has returned
  assert.equal(p2ack, true);

  // The one for p3, not
  assert.equal(p3ack, false);

  // p2 finishes
  p2res(null);

  // Wait for a task because why not
  await sleep(0);

  // p3 has started
  assert.equal(p3flag, true);

  // The calls to push p3 has returned
  assert.equal(p3ack, true);

  // p3 finishes
  p3res(null);

  // Wait for a microtask to let listeners fire
  await Promise.resolve();

  // Push a fourth event to test that the state has been reset
  let p4flag = false;

  // The push must return right away, as there are no
  // other operations queued.
  await train.push(() => {
    p4flag = true;
    return Promise.resolve();
  });

  // Must also start p4
  assert.equal(p4flag, true);
});
