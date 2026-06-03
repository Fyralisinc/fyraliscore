import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

const API_DIR = join(process.cwd(), "src", "api");
const RAW_HTTP_PATH =
  /(["'`])\/(?:v1|view|today|map|model|finance|slack|debug|webhooks|auth|ingest|observations|models|commitments|goals|decisions|resources)\b/g;

const CLIENT_FILES = [
  ...readdirSync(API_DIR)
    .filter((name) => name.endsWith("-client.ts"))
    .map((name) => join(API_DIR, name)),
  join(API_DIR, "client.ts"),
  join(API_DIR, "recommendation-stream.ts"),
  join(process.cwd(), "src", "pages", "model-v2", "api.ts"),
];

describe("API route contract hygiene", () => {
  it("keeps production API clients on the central route registry", () => {
    const violations: string[] = [];

    for (const file of CLIENT_FILES) {
      const source = readFileSync(file, "utf8");
      for (const match of source.matchAll(RAW_HTTP_PATH)) {
        const line =
          source.slice(0, match.index ?? 0).split("\n").length;
        violations.push(`${file.replace(process.cwd() + "/", "")}:${line}`);
      }
    }

    expect(violations).toEqual([]);
  });
});
