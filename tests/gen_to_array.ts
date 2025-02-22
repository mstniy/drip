export async function genToArray<T>(
  gen: AsyncGenerator<T, void, void>
): Promise<T[]> {
  const res: T[] = [];
  for await (const t of gen) {
    res.push(t);
  }
  return res;
}
