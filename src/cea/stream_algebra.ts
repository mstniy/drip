export async function* streamAddBinary<T1, T2>(
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
    // Delegate to the second stream
    yield res2.value;
    yield* s2;
  } else if (!res1.done && res2.done) {
    // Delegate to the first stream
    yield res1.value;
    yield* s1;
  }
}

type AGYieldType<T extends AsyncGenerator<unknown, unknown, unknown>> =
  T extends AsyncGenerator<infer TT, unknown, unknown> ? TT : unknown;

async function* emptyGenerator() {}

export function streamAdd<
  Generators extends readonly AsyncGenerator<unknown, void, void>[]
>(
  ss: Generators,
  lt: (
    a: AGYieldType<Generators[number]>,
    b: AGYieldType<Generators[number]>
  ) => boolean
): AsyncGenerator<AGYieldType<Generators[number]>, void, void> {
  type AGenerator = AsyncGenerator<AGYieldType<Generators[number]>, void, void>;

  if (ss.length === 0) {
    return emptyGenerator();
  }
  if (ss.length === 1) {
    return ss[0] as AGenerator;
  }
  return streamAddBinary(
    ss[0]! as AGenerator,
    streamAdd<AGenerator[]>(ss.slice(1) as AGenerator[], lt),
    lt
  );
}

export async function* streamSquashMerge<
  Generators extends readonly AsyncGenerator<unknown, void, void>[]
>(
  ss: Generators,
  lt: (
    a: AGYieldType<Generators[number]>,
    b: AGYieldType<Generators[number]>
  ) => boolean
): AsyncGenerator<AGYieldType<Generators[number]>, void, void> {
  const ress = await Promise.all(
    ss.map(
      (s) =>
        s.next() as Promise<
          IteratorResult<AGYieldType<Generators[number]>, void>
        >
    )
  );
  while (true) {
    let state:
      | { idxs: undefined }
      | { idxs: number[]; smallest: AGYieldType<Generators[number]> } = {
      idxs: undefined,
    };
    for (const [idx, res] of ress.entries()) {
      if (res.done) continue;
      if (typeof state.idxs === "undefined" || lt(res.value, state.smallest)) {
        state = {
          idxs: [idx],
          smallest: res.value,
        };
      } else if (!lt(state.smallest, res.value)) {
        state = {
          idxs: [...state.idxs, idx],
          smallest: state.smallest,
        };
      }
    }
    if (typeof state.idxs === "undefined") {
      // All streams are done
      break;
    }
    // Yield the smallest element, giving priority
    // to the instance coming from earlier streams
    // if there are multiple minimums
    yield state.smallest;
    // Advance the relevant stream(s)
    const news = await Promise.all(
      state.idxs.map(
        (idx) =>
          ss[idx]!.next() as Promise<
            IteratorResult<AGYieldType<Generators[number]>, void>
          >
      )
    );
    for (const [i, idx] of state.idxs.entries()) {
      ress[idx] = news[i]!;
    }
  }
}
