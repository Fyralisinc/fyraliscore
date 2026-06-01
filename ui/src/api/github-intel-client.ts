// GitHub Intelligence read API client.
//
// Typed wrappers over the gateway's /github-intel/* endpoints. Mirrors the
// request<T>() pattern in client.ts (which keeps its wrapper private): BASE
// "/api" → vite proxy strips /api → gateway; injects the Bearer token from
// localStorage["demoAuthToken"]; redirects to /demo on 401. Repos are addressed
// as {owner}/{repo} path segments.
import { ApiError } from "./client";
import { getAuthHeader } from "./auth";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

// NOTE: unlike the CEO-view clients, this one does NOT call handleAuthFailure()
// on 401 — that helper redirects to /demo, which would whisk the user away from
// the token bar before they can paste a token. Here a 401 is surfaced as an
// ApiError and the page keeps showing the (always-visible) token bar.
async function request<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "content-type": "application/json", ...getAuthHeader() },
    signal,
  });
  if (!res.ok) {
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

function repoPath(repo: string, suffix = ""): string {
  // repo is "owner/name"; encode each segment but keep the slash so it maps
  // onto the {owner}/{repo} path params.
  const [owner, ...rest] = repo.split("/");
  const name = rest.join("/");
  return `/github-intel/repos/${encodeURIComponent(owner)}/${encodeURIComponent(name)}${suffix}`;
}

// ---- response types (mirror services/github_intel/read_repo.py) ----------
export interface RepoSummary {
  repo: string;
  signal_count: number;
  last_signal_at: string | null;
  indexed: boolean;
  head_commit_sha: string | null;
  symbol_count: number;
  file_count: number;
}
export interface ReposResponse {
  repos: RepoSummary[];
  count: number;
}

export interface PrState {
  pr_number: number;
  lifecycle: string;
  ci_state: string;
  merged: boolean;
  base_ref: string | null;
  head_ref: string | null;
  head_sha?: string | null;
  title: string | null;
  author: string | null;
  opened_at?: string | null;
  closed_at?: string | null;
  updated_at: string | null;
}
export interface IssueState {
  issue_number: number;
  status: string;
  title: string | null;
  author: string | null;
  updated_at: string | null;
}
export interface BranchState {
  branch: string;
  head_sha: string | null;
  is_deleted: boolean;
  last_push_by: string | null;
}
export interface RepoStateResponse {
  repo: string;
  default_branch: string | null;
  head_sha: string | null;
  head_sha_at: string | null;
  code_index: {
    indexed: boolean;
    commit_sha: string | null;
    symbol_count: number;
    file_count: number;
    edge_count: number;
    indexed_at: string | null;
  };
  pull_requests: PrState[];
  issues: IssueState[];
  branches: BranchState[];
}

export interface SignalItem {
  observation_id: string;
  event_type: string;
  action: string | null;
  entity_kind: string | null;
  entity_ref: string | null;
  state_before: Record<string, unknown> | null;
  state_after: Record<string, unknown> | null;
  state_changed: boolean;
  cause: string | null;
  effect: string | null;
  confidence: number | null;
  reasoning_path: string | null;
  blast_radius_count: number;
  content_text: string | null;
  occurred_at: string | null;
  enriched_at: string | null;
}
export interface SignalsResponse {
  signals: SignalItem[];
  count: number;
  next_before: string | null;
}

export interface RelatedEntity {
  kind?: string;
  ref?: string;
  relation?: string;
}
export interface DependentSymbol {
  qualified_name: string;
  kind: string;
  path: string;
}
export interface EnrichmentDetail {
  event_type: string;
  action: string | null;
  entity_kind: string | null;
  entity_ref: string | null;
  state_before: Record<string, unknown> | null;
  state_after: Record<string, unknown> | null;
  state_changed: boolean;
  affected_files: string[] | null;
  affected_symbols: string[] | null;
  blast_radius: Record<string, unknown> | null;
  code_snapshot_sha: string | null;
  related_entities: RelatedEntity[] | null;
  cause: string | null;
  effect: string | null;
  explanation: string | null;
  confidence: number | null;
  reasoning_path: string | null;
  enriched_at: string | null;
}
export interface ExplainResponse {
  observation_id: string;
  content_text: string | null;
  occurred_at: string | null;
  intelligence: Record<string, unknown> | null;
  repo?: string;
  enrichment: EnrichmentDetail | null;
}

export interface BlastDependent {
  path: string;
  hops: number;
}
export interface BlastRadiusResponse {
  repo: string;
  indexed: boolean;
  reason?: string;
  commit_sha?: string;
  changed_files?: string[];
  unknown_files?: string[];
  dependent_files?: BlastDependent[];
  dependent_symbols?: DependentSymbol[];
  changed_symbols?: Array<{ qualified_name: string; kind: string }>;
}

export interface CodeSearchHit {
  qualified_name: string;
  kind: string;
  path: string;
  signature: string | null;
  score: number;
}
export interface CodeSearchResponse {
  repo: string;
  query: string;
  results: CodeSearchHit[];
  count?: number;
  indexed?: boolean;
}

// ---- endpoint functions --------------------------------------------------
export function listRepos(signal?: AbortSignal): Promise<ReposResponse> {
  return request<ReposResponse>("/github-intel/repos", signal);
}

export function getRepoState(repo: string, signal?: AbortSignal): Promise<RepoStateResponse> {
  return request<RepoStateResponse>(repoPath(repo, "/state"), signal);
}

export function getSignals(
  repo: string,
  opts: { limit?: number; before?: string; eventType?: string } = {},
  signal?: AbortSignal,
): Promise<SignalsResponse> {
  const qs = new URLSearchParams();
  if (opts.limit) qs.set("limit", String(opts.limit));
  if (opts.before) qs.set("before", opts.before);
  if (opts.eventType) qs.set("event_type", opts.eventType);
  const suffix = `/signals${qs.toString() ? `?${qs}` : ""}`;
  return request<SignalsResponse>(repoPath(repo, suffix), signal);
}

export function explainSignal(observationId: string, signal?: AbortSignal): Promise<ExplainResponse> {
  return request<ExplainResponse>(
    `/github-intel/signals/${encodeURIComponent(observationId)}/explain`,
    signal,
  );
}

export function getBlastRadius(
  repo: string,
  paths: string[],
  signal?: AbortSignal,
): Promise<BlastRadiusResponse> {
  const qs = new URLSearchParams();
  for (const p of paths) qs.append("path", p);
  return request<BlastRadiusResponse>(repoPath(repo, `/blast-radius?${qs}`), signal);
}

export function codeSearch(
  repo: string,
  q: string,
  k = 8,
  signal?: AbortSignal,
): Promise<CodeSearchResponse> {
  const qs = new URLSearchParams({ q, k: String(k) });
  return request<CodeSearchResponse>(repoPath(repo, `/code-search?${qs}`), signal);
}
