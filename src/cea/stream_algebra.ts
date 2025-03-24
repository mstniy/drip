type AGYieldType<T extends AsyncGenerator<unknown, unknown, unknown>> =
  T extends AsyncGenerator<infer TT, unknown, unknown> ? TT : unknown;

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
