// Shared primitives across the spec-aligned wire contracts.

export type ID = string;

export type EntityKind =
  | "actor"
  | "team"
  | "customer"
  | "source"
  | "node"
  | "thread"
  | "commitment"
  | "forecast"
  | "decision"
  | "delta"
  | "goal"
  | "risk";

export interface EntityRef {
  id: ID;
  type: EntityKind;
  label: string;
  // Optional disambiguating subtitle ("VP Engineering", "$1.2M ARR").
  subtitle?: string;
}

export interface Money {
  amount: number;
  currency?: string;  // ISO 4217; defaults to USD client-side.
}
