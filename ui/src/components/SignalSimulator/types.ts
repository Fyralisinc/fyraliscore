export type SendStatus =
  | { kind: "idle" }
  | { kind: "sending" }
  | { kind: "sent"; deduped: boolean; observation_id: string }
  | { kind: "error"; message: string };

export type SendFn = (
  channel: string,
  payload: Record<string, unknown>
) => Promise<void>;

export type TabId =
  | "slack"
  | "email"
  | "github"
  | "calendar"
  | "stripe"
  | "custom";
