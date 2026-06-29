"use client";

import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  DatabaseZap,
  KeyRound,
  Loader2,
  RefreshCw,
  ShieldCheck,
  UsersRound
} from "lucide-react";
import { useMemo, useState, type ReactNode } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Field, Input } from "@/components/ui/form";
import type {
  AgentFleetItem,
  ControlPanelAccessGrantList,
  ControlPanelAction,
  ControlPanelSection,
  ControlPanelState,
  DeploymentOption,
  ProductHealth,
  ProductSourceHealth,
  ReceiptList,
  ReceiptRecord
} from "@/src/types";

import {
  defaultControlPanelApiBase,
  fetchControlPanelDeployments,
  fetchControlPanelState
} from "../api";
import {
  SAMPLE_CONTROL_PANEL_STATE,
  SAMPLE_DEPLOYMENT_OPTIONS,
  SAMPLE_DEPLOYMENTS
} from "../data/mock-control-panel";

const SECTION_LABELS: Record<ControlPanelSection["key"], string> = {
  deployment_overview: "Overview",
  agent_fleet: "Agents",
  product_health: "Product health",
  evidence_packages: "Evidence",
  preflight_reports: "Preflight",
  runner_evidence: "Runner"
};

const ACTION_LABELS: Record<ControlPanelAction["code"], string> = {
  enroll_agent: "Enroll agent",
  restore_agent_health: "Restore agent health",
  submit_evidence_package: "Submit evidence package",
  review_evidence_failures: "Review evidence failures",
  review_preflight_failures: "Review preflight failures",
  review_runner_failures: "Review runner failures",
  review_desired_state_drift: "Review desired-state drift",
  review_product_health: "Review product health"
};

export function ControlPanelApp() {
  const [apiBase, setApiBase] = useState(defaultControlPanelApiBase());
  const [bearerToken, setBearerToken] = useState("");
  const [customerFilter, setCustomerFilter] = useState("cus_acme_finance");
  const [recentLimit, setRecentLimit] = useState(10);
  const [deployments, setDeployments] =
    useState<ControlPanelAccessGrantList>(SAMPLE_DEPLOYMENTS);
  const [deploymentOptions, setDeploymentOptions] = useState<DeploymentOption[]>(
    SAMPLE_DEPLOYMENT_OPTIONS
  );
  const [selected, setSelected] = useState<DeploymentOption>(
    SAMPLE_DEPLOYMENT_OPTIONS[0]
  );
  const [state, setState] = useState<ControlPanelState>(
    SAMPLE_CONTROL_PANEL_STATE
  );
  const [mode, setMode] = useState<"sample" | "live">("sample");
  const [loading, setLoading] = useState<"deployments" | "state" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const generatedAt = useMemo(
    () => formatDateTime(state.generated_at),
    [state.generated_at]
  );

  async function loadDeployments() {
    setLoading("deployments");
    setError(null);
    try {
      const nextDeployments = await fetchControlPanelDeployments(
        { apiBase, bearerToken },
        customerFilter || undefined
      );
      const nextOptions = flattenDeploymentOptions(nextDeployments);
      setDeployments(nextDeployments);
      setDeploymentOptions(nextOptions);
      setMode("live");
      if (nextOptions[0]) {
        await loadState(nextOptions[0], { keepMode: true });
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(null);
    }
  }

  async function loadState(
    option = selected,
    flags: { keepMode?: boolean } = {}
  ) {
    setLoading("state");
    setError(null);
    setSelected(option);
    try {
      const nextState = await fetchControlPanelState(
        { apiBase, bearerToken },
        {
          deploymentId: option.deploymentId,
          customerId: option.customerId,
          recentLimit
        }
      );
      setState(nextState);
      if (!flags.keepMode) {
        setMode("live");
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(null);
    }
  }

  function resetSample() {
    setDeployments(SAMPLE_DEPLOYMENTS);
    setDeploymentOptions(SAMPLE_DEPLOYMENT_OPTIONS);
    setSelected(SAMPLE_DEPLOYMENT_OPTIONS[0]);
    setState(SAMPLE_CONTROL_PANEL_STATE);
    setError(null);
    setMode("sample");
  }

  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader className="items-start">
          <div>
            <CardTitle>Metadata-only control panel</CardTitle>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
              This preserves the legacy BYOC control-panel UI as a modern Next surface. The bearer token is held in memory only, and the client rejects responses that do not declare sanitized stored scopes.
            </p>
          </div>
          <Badge tone={mode === "live" ? "success" : "info"}>
            {mode === "live" ? "Live gateway" : "Sample state"}
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-4 xl:grid-cols-[1fr_auto]">
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <Field label="Gateway API base" help="Blank uses the same origin.">
              <Input
                value={apiBase}
                onChange={(event) => setApiBase(event.target.value)}
                placeholder="https://api.fyralis.com"
              />
            </Field>
            <Field label="Bearer token" help="Not stored in browser storage.">
              <Input
                value={bearerToken}
                onChange={(event) => setBearerToken(event.target.value)}
                type="password"
                autoComplete="off"
                placeholder="Paste operator token"
              />
            </Field>
            <Field label="Customer" help="Optional customer filter.">
              <Input
                value={customerFilter}
                onChange={(event) => setCustomerFilter(event.target.value)}
                placeholder="cus_..."
              />
            </Field>
            <Field label="Recent receipts" help="Receipt rows per section.">
              <Input
                min={1}
                max={20}
                type="number"
                value={recentLimit}
                onChange={(event) =>
                  setRecentLimit(Number(event.target.value || 10))
                }
              />
            </Field>
          </div>
          <div className="flex flex-wrap items-end gap-2 xl:justify-end">
            <Button
              type="button"
              onClick={() => void loadDeployments()}
              disabled={loading !== null}
            >
              {loading === "deployments" ? (
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
              ) : (
                <RefreshCw className="h-4 w-4" aria-hidden="true" />
              )}
              Load gateway state
            </Button>
            <Button type="button" variant="secondary" onClick={resetSample}>
              Use sample
            </Button>
          </div>
        </CardContent>
      </Card>

      {error ? (
        <div className="flex items-start gap-3 rounded-lg border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden="true" />
          <span>{error}</span>
        </div>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-[19rem_minmax(0,1fr)]">
        <aside className="grid h-fit gap-3 rounded-lg border border-border bg-card p-4 shadow-panel">
          <div className="flex items-center justify-between gap-3">
            <strong className="text-sm">Deployments</strong>
            <Badge tone="muted">{deployments.result_count}</Badge>
          </div>
          <div className="grid gap-2">
            {deploymentOptions.length === 0 ? (
              <div className="rounded-md border border-border bg-background/70 p-3 text-sm text-muted-foreground">
                No deployment access loaded.
              </div>
            ) : (
              deploymentOptions.map((option) => (
                <button
                  key={`${option.customerId}:${option.deploymentId}`}
                  className={
                    selected.deploymentId === option.deploymentId
                      ? "rounded-md border border-primary bg-primary px-3 py-3 text-left text-primary-foreground"
                      : "rounded-md border border-border bg-background/70 px-3 py-3 text-left transition hover:border-ring hover:bg-accent"
                  }
                  onClick={() => void loadState(option)}
                  type="button"
                >
                  <strong className="block truncate text-sm">
                    {option.deploymentId}
                  </strong>
                  <span className="mt-1 block truncate text-xs opacity-80">
                    {option.customerId}
                  </span>
                  <span className="mt-3 inline-flex rounded-full border border-current/20 px-2 py-0.5 text-xs font-bold">
                    {option.role}
                  </span>
                </button>
              ))
            )}
          </div>
        </aside>

        <section className="min-w-0">
          <DeploymentStateView
            state={state}
            generatedAt={generatedAt}
            onRefresh={() => void loadState()}
            refreshDisabled={loading !== null}
          />
        </section>
      </div>
    </div>
  );
}

function DeploymentStateView({
  state,
  generatedAt,
  onRefresh,
  refreshDisabled
}: {
  state: ControlPanelState;
  generatedAt: string;
  onRefresh: () => void;
  refreshDisabled: boolean;
}) {
  const overview = state.overview;
  const summaryCards = [
    {
      label: "Agents",
      value: overview.agent_summary.enrolled_count,
      detail: `${overview.agent_summary.passing_count} passing`,
      icon: UsersRound
    },
    {
      label: "Evidence",
      value: overview.evidence_summary.receipt_count,
      detail: latestStatus(overview.evidence_summary.latest_ledger_status),
      icon: ShieldCheck
    },
    {
      label: "Preflight",
      value: overview.preflight_summary.receipt_count,
      detail: latestStatus(overview.preflight_summary.latest_preflight_status),
      icon: CheckCircle2
    },
    {
      label: "Runner",
      value: overview.runner_summary.receipt_count,
      detail: latestStatus(overview.runner_summary.latest_runner_status),
      icon: Activity
    }
  ];

  return (
    <div className="grid gap-5">
      <Card>
        <CardContent className="grid gap-4 md:grid-cols-[1fr_auto] md:items-center">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <Badge tone={statusTone(overview.status)}>{overview.status}</Badge>
              <Badge tone="muted">{state.stored_scope}</Badge>
            </div>
            <h2 className="mt-3 text-3xl font-semibold tracking-tight">
              {state.deployment_id}
            </h2>
            <p className="mt-2 text-sm leading-6 text-muted-foreground">
              {state.customer_id ?? "unknown customer"} | generated {generatedAt}
            </p>
          </div>
          <div className="grid gap-2 rounded-lg border border-border bg-background/70 p-4">
            <span className="text-xs font-semibold text-muted-foreground">
              Next action
            </span>
            <strong className="text-sm">{formatCode(overview.next_action)}</strong>
            <Button
              type="button"
              variant="secondary"
              onClick={onRefresh}
              disabled={refreshDisabled}
            >
              {refreshDisabled ? (
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
              ) : (
                <RefreshCw className="h-4 w-4" aria-hidden="true" />
              )}
              Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {summaryCards.map((card) => {
          const Icon = card.icon;
          return (
            <MetricCard key={card.label} label={card.label} value={card.value} detail={card.detail}>
              <Icon className="h-4 w-4" aria-hidden="true" />
            </MetricCard>
          );
        })}
      </section>

      <ProductHealthPanel health={state.product_health} />

      <section className="grid gap-5 xl:grid-cols-2">
        <Panel title="Sections">
          <div className="grid gap-2">
            {state.sections.map((section) => (
              <SectionRow key={section.key} section={section} />
            ))}
          </div>
        </Panel>

        <Panel title="Actions">
          {state.actions.length === 0 ? (
            <EmptyState>No open actions</EmptyState>
          ) : (
            <div className="grid gap-2">
              {state.actions.map((action) => (
                <div
                  className="flex items-center justify-between gap-3 rounded-md border border-border bg-background/70 p-3"
                  key={action.code}
                >
                  <span className="min-w-0">
                    <strong className="block truncate text-sm">
                      {ACTION_LABELS[action.code]}
                    </strong>
                    <span className="text-xs font-medium text-muted-foreground">
                      {SECTION_LABELS[action.target_section]}
                    </span>
                  </span>
                  <Badge tone={priorityTone(action.priority)}>
                    {action.priority}
                  </Badge>
                </div>
              ))}
            </div>
          )}
        </Panel>
      </section>

      <Panel title="Agent fleet">
        <AgentTable items={state.agent_fleet.items} />
      </Panel>

      <section className="grid gap-5 xl:grid-cols-3">
        <ReceiptPanel title="Evidence packages" list={state.evidence_packages} />
        <ReceiptPanel title="Preflight reports" list={state.preflight_reports} />
        <ReceiptPanel title="Runner evidence" list={state.runner_evidence} />
      </section>
    </div>
  );
}

function ProductHealthPanel({ health }: { health: ProductHealth }) {
  const totalIngested = health.sources.reduce(
    (total, source) => total + source.items_ingested_count,
    0
  );
  const readySources = health.sources.filter(
    (source) => source.status === "ready"
  ).length;

  return (
    <Panel title="Product health">
      <div className="grid gap-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <span className="flex flex-wrap items-center gap-2">
            <Badge tone={statusTone(health.overall_status)}>
              {health.overall_status}
            </Badge>
            <span className="text-sm text-muted-foreground">
              {health.observed
                ? `observed ${formatDateTime(health.latest_collected_at)}`
                : "not observed"}
            </span>
          </span>
          <span className="flex flex-wrap gap-2">
            <Badge tone={statusTone(health.pipeline.status)}>pipeline</Badge>
            <Badge tone={statusTone(health.think.status)}>think</Badge>
            <Badge tone={statusTone(health.models.status)}>models</Badge>
            <Badge tone={statusTone(health.vector_index.status)}>vectors</Badge>
          </span>
        </div>

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <CompactMetric label="Ingested" value={totalIngested} detail={`${readySources}/${health.sources.length} sources ready`} />
          <CompactMetric label="Think runs" value={health.think.run_count} detail={`${health.think.failed_run_count} failed`} />
          <CompactMetric label="Models" value={health.models.model_count} detail={`${health.models.model_relation_count} relations`} />
          <CompactMetric label="Vector backlog" value={health.vector_index.backlog_count} detail={`${health.vector_index.vector_count} vectors`} />
        </div>

        <div className="grid gap-5 xl:grid-cols-[1.35fr_0.65fr]">
          <div className="min-w-0">
            <h3 className="mb-3 text-sm font-semibold">Sources</h3>
            <SourceHealthTable sources={health.sources} />
          </div>
          <div className="min-w-0">
            <h3 className="mb-3 text-sm font-semibold">Open issues</h3>
            {health.issues.length === 0 ? (
              <EmptyState>No open product issues</EmptyState>
            ) : (
              <div className="grid gap-2">
                {health.issues.slice(0, 6).map((issue) => (
                  <div
                    className="rounded-md border border-border bg-background/70 p-3"
                    key={`${issue.component}:${issue.code}`}
                  >
                    <Badge tone={priorityTone(issue.severity)}>
                      {issue.severity}
                    </Badge>
                    <strong className="mt-2 block text-sm">
                      {formatCode(issue.code)}
                    </strong>
                    <span className="text-xs text-muted-foreground">
                      {formatCode(issue.component)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </Panel>
  );
}

function SourceHealthTable({ sources }: { sources: ProductSourceHealth[] }) {
  if (sources.length === 0) {
    return <EmptyState>No source snapshots</EmptyState>;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[42rem] border-collapse text-left text-sm">
        <thead className="bg-muted text-xs uppercase text-muted-foreground">
          <tr>
            <th className="px-3 py-2">Source</th>
            <th className="px-3 py-2">Status</th>
            <th className="px-3 py-2">Ingested</th>
            <th className="px-3 py-2">Queue</th>
            <th className="px-3 py-2">Last success</th>
          </tr>
        </thead>
        <tbody>
          {sources.map((source) => (
            <tr key={source.source} className="border-t border-border bg-card">
              <td className="px-3 py-3">
                <strong className="block">{source.source}</strong>
                <span className="text-xs text-muted-foreground">
                  {formatCode(source.backfill_status)}
                </span>
              </td>
              <td className="px-3 py-3">
                <Badge tone={statusTone(source.status)}>{source.status}</Badge>
              </td>
              <td className="px-3 py-3">
                {source.items_ingested_count.toLocaleString()}
                <span className="block text-xs text-muted-foreground">
                  {source.items_failed_count} failed
                </span>
              </td>
              <td className="px-3 py-3">
                {source.queue_depth_count.toLocaleString()}
                <span className="block text-xs text-muted-foreground">
                  {source.lag_seconds === null
                    ? "lag unknown"
                    : `${source.lag_seconds}s lag`}
                </span>
              </td>
              <td className="px-3 py-3">{formatDateTime(source.last_success_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}

function SectionRow({ section }: { section: ControlPanelSection }) {
  return (
    <div className="grid gap-3 rounded-md border border-border bg-background/70 p-3 sm:grid-cols-[1fr_auto_auto] sm:items-center">
      <div className="min-w-0">
        <strong className="block truncate text-sm">{SECTION_LABELS[section.key]}</strong>
        <span className="text-xs text-muted-foreground">
          {formatDateTime(section.latest_observed_at)}
        </span>
      </div>
      <span className="text-sm font-semibold">{section.item_count}</span>
      <Badge tone={statusTone(section.status)}>{section.status}</Badge>
    </div>
  );
}

function AgentTable({ items }: { items: AgentFleetItem[] }) {
  if (items.length === 0) {
    return <EmptyState>No enrolled agents</EmptyState>;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[52rem] border-collapse text-left text-sm">
        <thead className="bg-muted text-xs uppercase text-muted-foreground">
          <tr>
            <th className="px-3 py-2">Agent</th>
            <th className="px-3 py-2">Region</th>
            <th className="px-3 py-2">Revision</th>
            <th className="px-3 py-2">Validation</th>
            <th className="px-3 py-2">Heartbeat</th>
            <th className="px-3 py-2">Queue</th>
          </tr>
        </thead>
        <tbody>
          {items.map((agent) => (
            <tr key={agent.agent_id} className="border-t border-border bg-card">
              <td className="px-3 py-3">
                <strong className="block">{agent.agent_id}</strong>
                <span className="text-xs text-muted-foreground">
                  {agent.agent_version}
                </span>
              </td>
              <td className="px-3 py-3">{agent.region}</td>
              <td className="px-3 py-3">{agent.desired_revision}</td>
              <td className="px-3 py-3">
                <Badge tone={validationTone(agent.latest_validation_status)}>
                  {agent.latest_validation_status ?? "unknown"}
                </Badge>
              </td>
              <td className="px-3 py-3">
                {formatDateTime(agent.latest_heartbeat_accepted_at)}
              </td>
              <td className="px-3 py-3">
                {(agent.latest_queued_batches ?? 0).toLocaleString()} queued
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ReceiptPanel({ title, list }: { title: string; list: ReceiptList }) {
  return (
    <Panel title={title}>
      {list.items.length === 0 ? (
        <EmptyState>No receipts</EmptyState>
      ) : (
        <div className="grid gap-2">
          {list.items.slice(0, 4).map((item, index) => (
            <ReceiptRow item={item} key={item.receipt?.receipt_id ?? index} />
          ))}
        </div>
      )}
    </Panel>
  );
}

function ReceiptRow({ item }: { item: ReceiptRecord }) {
  const receipt = item.receipt ?? {};
  const status =
    receipt.ledger_overall_status ??
    receipt.preflight_status ??
    receipt.runner_status ??
    receipt.status ??
    "unknown";
  return (
    <div className="flex items-center justify-between gap-3 rounded-md border border-border bg-background/70 p-3">
      <span className="min-w-0">
        <strong className="block truncate text-sm">
          {receipt.receipt_id ?? "receipt"}
        </strong>
        <span className="text-xs text-muted-foreground">
          {formatDateTime(receipt.accepted_at ?? item.submitted_at)}
        </span>
      </span>
      <Badge tone={receiptTone(String(status))}>{String(status)}</Badge>
    </div>
  );
}

function MetricCard({
  label,
  value,
  detail,
  children
}: {
  label: string;
  value: number;
  detail: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-5 shadow-panel">
      <div className="flex items-center gap-2 text-xs font-semibold text-muted-foreground">
        <span className="text-info">{children}</span>
        {label}
      </div>
      <strong className="mt-3 block text-3xl tracking-tight">
        {value.toLocaleString()}
      </strong>
      <p className="mt-1 text-sm text-muted-foreground">{detail}</p>
    </div>
  );
}

function CompactMetric({
  label,
  value,
  detail
}: {
  label: string;
  value: number;
  detail: string;
}) {
  return (
    <div className="rounded-md border border-border bg-background/70 p-3">
      <span className="text-xs font-semibold text-muted-foreground">{label}</span>
      <strong className="mt-1 block text-xl">{value.toLocaleString()}</strong>
      <span className="text-xs text-muted-foreground">{detail}</span>
    </div>
  );
}

function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-md border border-dashed border-border bg-background/70 p-4 text-sm text-muted-foreground">
      {children}
    </div>
  );
}

function flattenDeploymentOptions(
  grantList: ControlPanelAccessGrantList
): DeploymentOption[] {
  return grantList.items.flatMap((grant) =>
    grant.deployment_ids.map((deploymentId) => ({
      customerId: grant.customer_id,
      deploymentId,
      role: grant.role,
      expiresAt: grant.expires_at
    }))
  );
}

function statusTone(status: string): "success" | "warning" | "error" | "muted" | "info" {
  if (status === "ready" || status === "passed" || status === "passing") {
    return "success";
  }
  if (status === "degraded" || status === "unknown" || status === "empty") {
    return "warning";
  }
  if (status === "action_required" || status === "failing" || status === "open") {
    return "error";
  }
  return "muted";
}

function validationTone(status: string | null): "success" | "warning" | "error" | "muted" {
  if (status === "passing") {
    return "success";
  }
  if (status === "degraded") {
    return "warning";
  }
  if (status === "failing") {
    return "error";
  }
  return "muted";
}

function priorityTone(priority: string): "success" | "warning" | "error" | "info" {
  if (priority === "critical") {
    return "error";
  }
  if (priority === "warning") {
    return "warning";
  }
  return "info";
}

function receiptTone(status: string): "success" | "warning" | "error" | "muted" {
  const normalized = status.toLowerCase();
  if (normalized.includes("pass") || normalized === "accepted") {
    return "success";
  }
  if (normalized.includes("fail")) {
    return "error";
  }
  if (normalized.includes("skip")) {
    return "warning";
  }
  return "muted";
}

function latestStatus(value: string | null | undefined): string {
  return value ? formatCode(value) : "none";
}

function formatCode(value: string | null | undefined): string {
  if (!value) {
    return "unknown";
  }
  return value.replaceAll("_", " ");
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return "not observed";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : "Unknown control-panel error";
}
