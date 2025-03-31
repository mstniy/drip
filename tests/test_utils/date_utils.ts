export function incrementDate(d: Date): Date {
  return advanceDate(d, 1);
}

export const ONE_YEAR_MS = 365 * 24 * 3600 * 1000;

export function advanceDate(d: Date, ms: number): Date {
  const res = new Date();
  res.setTime(d.getTime() + ms);
  return res;
}
