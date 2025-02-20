import { ObjectId, Timestamp } from "mongodb";
import { oidLT } from "./oid_less";

type HasOrder = {
  ct: Timestamp;
  _id: ObjectId;
};

function lt(a: HasOrder, b: HasOrder) {
  return a.ct.lt(b.ct) || (a.ct.eq(b.ct) && oidLT(a._id, b._id));
}

export async function* addCS<T1 extends HasOrder, T2 extends HasOrder>(
  c1: AsyncGenerator<T1, void, void>,
  c2: AsyncGenerator<T2, void, void>
): AsyncGenerator<T1 | T2, void, void> {
  let [res1, res2] = await Promise.all([c1.next(), c2.next()]);
  while (!res1.done && !res2.done) {
    const val1 = res1.value;
    const val2 = res2.value;
    if (lt(val1, val2)) {
      yield val1;
      res1 = await c1.next();
    } else {
      yield val2;
      res2 = await c2.next();
    }
  }
  if (res1.done && !res2.done) {
    // Delegate to thethe second stream
    yield res2.value;
    yield* c2;
  } else if (!res1.done && res2.done) {
    // Delegate to the first stream
    yield res1.value;
    yield* c1;
  }
}

export async function* subtractCS<T extends HasOrder>(
  c1: AsyncGenerator<T, void, void>,
  c2: AsyncGenerator<HasOrder, void, void>
): AsyncGenerator<T, void, void> {
  let [res1, res2] = await Promise.all([c1.next(), c2.next()]);
  while (!res1.done && !res2.done) {
    if (lt(res1.value, res2.value)) {
      yield res1.value;
      res1 = await c1.next();
    } else if (lt(res2.value, res1.value)) {
      res2 = await c2.next();
    } else {
      res1 = await c1.next();
    }
  }
  if (!res1.done) {
    yield res1.value;
    yield* c1;
  }
}
