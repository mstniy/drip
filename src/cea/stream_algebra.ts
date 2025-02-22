export async function* streamAdd<T1, T2>(
  s1: AsyncGenerator<T1, void, void>,
  s2: AsyncGenerator<T2, void, void>,
  lt: (a: T1, b: T2) => boolean
): AsyncGenerator<T1 | T2, void, void> {
  let [res1, res2] = await Promise.all([s1.next(), s2.next()]);
  while (!res1.done && !res2.done) {
    const val1 = res1.value;
    const val2 = res2.value;
    if (lt(val1, val2)) {
      yield val1;
      res1 = await s1.next();
    } else {
      yield val2;
      res2 = await s2.next();
    }
  }
  if (res1.done && !res2.done) {
    // Delegate to thethe second stream
    yield res2.value;
    yield* s2;
  } else if (!res1.done && res2.done) {
    // Delegate to the first stream
    yield res1.value;
    yield* s1;
  }
}

export async function* streamSubtract<T1, T2>(
  s1: AsyncGenerator<T1, void, void>,
  s2: AsyncGenerator<T2, void, void>,
  lt: (a: T1 | T2, b: T1 | T2) => boolean
): AsyncGenerator<T1, void, void> {
  let [res1, res2] = await Promise.all([s1.next(), s2.next()]);
  while (!res1.done && !res2.done) {
    if (lt(res1.value, res2.value)) {
      yield res1.value;
      res1 = await s1.next();
    } else if (lt(res2.value, res1.value)) {
      res2 = await s2.next();
    } else {
      res1 = await s1.next();
      res2 = await s2.next();
    }
  }
  if (!res1.done) {
    yield res1.value;
    yield* s1;
  }
  if (!res2.done) {
    s2.return();
  }
}
