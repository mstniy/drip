export async function genToArray<T>(
  gen: AsyncGenerator<T, unknown, void>
): Promise<T[]> {
  const res: T[] = [];
  for await (const t of gen) {
    res.push(t);
  }
  return res;
}
