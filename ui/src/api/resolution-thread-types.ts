export type ResolutionThreadStatus =
  | "draft"
  | "active"
  | "waiting_on_owner"
  | "blocked"
  | "monitoring"
  | "confirmed"
  | "resolved"
  | "failed";

export type ResolutionStepStatus =
  | "not_started"
  | "in_progress"
  | "waiting"
  | "blocked"
  | "done"
  | "failed";

export type ResolutionWatchedSignalStatus =
  | "watching"
  | "seen"
  | "missing"
  | "contradicted";

export interface ResolutionThreadStep {
  id: string;
  label: string;
  owner: string;
  status: ResolutionStepStatus;
  dueAt?: string | null;
  proofNeeded?: string | null;
  blockedBy?: string | null;
}

export interface ResolutionWatchedSignal {
  id: string;
  label: string;
  sourceType: string;
  expected: string;
  status: ResolutionWatchedSignalStatus;
  lastObservedAt?: string | null;
  matchedEvidence?: Record<string, unknown> | null;
}

export interface ResolutionThread {
  id: string;
  sourceDecisionDeltaId?: string | null;
  targetNodeKind?: string | null;
  targetNodeId?: string | null;
  title: string;
  status: ResolutionThreadStatus;
  currentState: string;
  targetState: string;
  owner: string;
  nextReviewAt?: string | null;
  successCriteria: string[];
  steps: ResolutionThreadStep[];
  watchedSignals: ResolutionWatchedSignal[];
  escalationTriggers: string[];
  createdAt?: string | null;
  updatedAt?: string | null;
  resolvedAt?: string | null;
  failedAt?: string | null;
}
