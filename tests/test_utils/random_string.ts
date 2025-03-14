import * as crypto from "node:crypto";

export function getRandomString(): string {
  return crypto.randomBytes(8).toString("hex");
}
