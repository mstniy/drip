/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-require-imports */

import { isBun } from "./is_bun";

export function describe(name: string, f: () => void | Promise<unknown>): void {
  if (isBun()) {
    require("bun:test").describe(name, f);
  } else {
    require("node:test").describe(name, f);
  }
}

export function it(
  name: string,
  f: () => void | Promise<unknown>,
  options?: { timeout: number }
): void {
  if (isBun()) {
    require("bun:test").it(name, f, options);
  } else {
    require("node:test").it(name, options, f);
  }
}

export function beforeEach(f: () => void | Promise<unknown>): void {
  if (isBun()) {
    require("bun:test").beforeEach(f);
  } else {
    require("node:test").beforeEach(f);
  }
}

export function afterEach(f: () => void | Promise<unknown>): void {
  if (isBun()) {
    require("bun:test").afterEach(f);
  } else {
    require("node:test").afterEach(f);
  }
}

export function before(f: () => void | Promise<unknown>): void {
  if (isBun()) {
    require("bun:test").beforeAll(f);
  } else {
    require("node:test").before(f);
  }
}

export function after(f: () => void | Promise<unknown>): void {
  if (isBun()) {
    require("bun:test").afterAll(f);
  } else {
    require("node:test").after(f);
  }
}
