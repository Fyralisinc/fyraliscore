import { type FormEvent, type ReactNode, useMemo, useState } from "react";

import {
  fetchControlPanelState,
  fetchDeployments,
  type ClientConfig
} from "./api";
import type {
  AgentFleetItem,
  ControlPanelAccessGrantList,
  ControlPanelAction,
  ControlPanelSection,
  ControlPanelState,
  DeploymentOption,
  ReceiptList,
  ReceiptRecord
} from "./types";

const DEFAULT_API_BASE = import.meta.env.VITE_FYRALIS_API_BASE ?? "";

const SECTION_LABELS: Record<ControlPanelSection["key"], string> = {
  deployment_overview: "Overview",
  agent_fleet: "Agents",
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
  review_desired_state_drift: "Review desired state drift"
};

export function App() {
  const [apiBase, setApiBase] = useState(DEFAULT_API_BASE);
  const [bearerToken, setBearerToken] = useState("");
  const [customerFilter, setCustomerFilter] = useState("");
  const [recentLimit, setRecentLimit] = useState(10);
  const [deployments, setDeployments] =
    useState<ControlPanelAccessGrantList | null>(null);
  const [selected, setSelected] = useState<DeploymentOption | null>(null);
  const [state, setState] = useState<ControlPanelState | null>(null);
  const [loading, setLoading] = useState<"deployments" | "state" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const clientConfig = useMemo<ClientConfig>(
    () => ({ apiBase, bearerToken }),
    [apiBase, bearerToken]
  );
  const deploymentOptions = useMemo(
    () => flattenDeploymentOptions(deployments),
    [deployments]
  );

  async function loadDeployments(event?: FormEvent) {
    event?.preventDefault();
    setLoading("deployments");
    setError(null);
    try {
      const nextDeployments = await fetchDeployments(
        clientConfig,
        customerFilter || undefined
      );
      const nextOptions = flattenDeploymentOptions(nextDeployments);
      setDeployments(nextDeployments);
      setState(null);
      const nextSelected = nextOptions[0] ?? null;
      setSelected(nextSelected);
      if (nextSelected) {
        await loadState(nextSelected, { keepSelection: true });
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(null);
    }
  }

  async function loadState(
    option = selected,
    flags: { keepSelection?: boolean } = {}
  ) {
    if (!option) {
      return;
    }
    if (!flags.keepSelection) {
      setSelected(option);
    }
    setLoading("state");
    setError(null);
    try {
      const nextState = await fetchControlPanelState(clientConfig, {
        deploymentId: option.deploymentId,
        customerId: option.customerId,
        recentLimit
      });
      setState(nextState);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setLoading(null);
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">BYOC</p>
          <h1>Control Panel</h1>
        </div>
        <form className="connection" onSubmit={loadDeployments}>
          <label>
            API base
            <input
              value={apiBase}
              onChange={(event) => setApiBase(event.target.value)}
              placeholder="same origin"
            />
          </label>
          <label>
            Bearer token
            <input
              value={bearerToken}
              onChange={(event) => setBearerToken(event.target.value)}
              type="password"
              autoComplete="off"
            />
          </label>
          <label className="short-field">
            Customer
            <input
              value={customerFilter}
              onChange={(event) => setCustomerFilter(event.target.value)}
              placeholder="cus_..."
            />
          </label>
          <label className="tiny-field">
            Limit
            <input
              min={1}
              max={20}
              type="number"
              value={recentLimit}
              onChange={(event) =>
                setRecentLimit(Number(event.target.value || 10))
              }
            />
          </label>
          <button type="submit" disabled={loading === "deployments"}>
            {loading === "deployments" ? "Loading" : "Refresh"}
          </button>
        </form>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <div className="workspace">
        <aside className="deployment-rail" aria-label="BYOC deployments">
          <div className="rail-head">
            <span>Deployments</span>
            <strong>{deploymentOptions.length}</strong>
          </div>
          <div className="deployment-list">
            {deploymentOptions.length === 0 ? (
              <div className="empty-state">No deployments loaded</div>
            ) : (
              deploymentOptions.map((option) => (
                <button
                  key={`${option.customerId}:${option.deploymentId}`}
                  className={
                    selected?.deploymentId === option.deploymentId
                      ? "deployment-row selected"
                      : "deployment-row"
                  }
                  onClick={() => loadState(option)}
                  type="button"
                >
                  <span>
                    <strong>{option.deploymentId}</strong>
                    <small>{option.customerId}</small>
                  </span>
                  <Pill tone={roleTone(option.role)}>{option.role}</Pill>
                </button>
              ))
            )}
          </div>
        </aside>

        <section className="content">
          {state ? (
            <DeploymentStateView
              state={state}
              onRefresh={() => loadState()}
              refreshDisabled={loading === "state"}
            />
          ) : (
            <div className="blank-panel">
              <h2>No deployment selected</h2>
              <p>Load deployment access to open the BYOC state view.</p>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}

function DeploymentStateView({
  state,
  onRefresh,
  refreshDisabled
}: {
  state: ControlPanelState;
  onRefresh: () => void;
  refreshDisabled: boolean;
}) {
  const overview = state.overview;
  const summaryCards = [
    {
      label: "Agents",
      value: overview.agent_summary.enrolled_count,
      detail: `${overview.agent_summary.passing_count} passing`
    },
    {
      label: "Evidence",
      value: overview.evidence_summary.receipt_count,
      detail: latestStatus(overview.evidence_summary.latest_ledger_status)
    },
    {
      label: "Preflight",
      value: overview.preflight_summary.receipt_count,
      detail: latestStatus(overview.preflight_summary.latest_preflight_status)
    },
    {
      label: "Runner",
      value: overview.runner_summary.receipt_count,
      detail: latestStatus(overview.runner_summary.latest_runner_status)
    }
  ];

  return (
    <>
      <section className="hero-panel">
        <div>
          <div className="title-row">
            <h2>{state.deployment_id}</h2>
            <Pill tone={statusTone(overview.status)}>{overview.status}</Pill>
          </div>
          <p className="muted-line">
            {state.customer_id ?? "unknown customer"} | generated{" "}
            {formatDateTime(state.generated_at)}
          </p>
        </div>
        <div className="hero-actions">
          <div>
            <small>Next action</small>
            <strong>{formatCode(overview.next_action)}</strong>
          </div>
          <button type="button" onClick={onRefresh} disabled={refreshDisabled}>
            {refreshDisabled ? "Refreshing" : "Refresh"}
          </button>
        </div>
      </section>

      <section className="metric-grid">
        {summaryCards.map((card) => (
          <div className="metric" key={card.label}>
            <span>{card.label}</span>
            <strong>{card.value}</strong>
            <small>{card.detail}</small>
          </div>
        ))}
      </section>

      <section className="split-grid">
        <Panel title="Sections">
          <div className="section-list">
            {state.sections.map((section) => (
              <SectionRow key={section.key} section={section} />
            ))}
          </div>
        </Panel>

        <Panel title="Actions">
          {state.actions.length === 0 ? (
            <div className="empty-state compact">No open actions</div>
          ) : (
            <div className="action-list">
              {state.actions.map((action) => (
                <div className="action-row" key={action.code}>
                  <Pill tone={priorityTone(action.priority)}>
                    {action.priority}
                  </Pill>
                  <span>{ACTION_LABELS[action.code]}</span>
                  <small>{SECTION_LABELS[action.target_section]}</small>
                </div>
              ))}
            </div>
          )}
        </Panel>
      </section>

      <Panel title="Agent Fleet">
        <AgentTable items={state.agent_fleet.items} />
      </Panel>

      <section className="split-grid">
        <ReceiptPanel title="Evidence Packages" list={state.evidence_packages} />
        <ReceiptPanel title="Preflight Reports" list={state.preflight_reports} />
        <ReceiptPanel title="Runner Evidence" list={state.runner_evidence} />
      </section>
    </>
  );
}

function Panel({
  title,
  children
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="panel">
      <h3>{title}</h3>
      {children}
    </section>
  );
}

function SectionRow({ section }: { section: ControlPanelSection }) {
  return (
    <div className="section-row">
      <div>
        <strong>{SECTION_LABELS[section.key]}</strong>
        <small>{formatDateTime(section.latest_observed_at)}</small>
      </div>
      <span>{section.item_count}</span>
      <Pill tone={statusTone(section.status)}>{section.status}</Pill>
    </div>
  );
}

function AgentTable({ items }: { items: AgentFleetItem[] }) {
  if (items.length === 0) {
    return <div className="empty-state compact">No enrolled agents</div>;
  }
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Agent</th>
            <th>Region</th>
            <th>Revision</th>
            <th>Validation</th>
            <th>Heartbeat</th>
            <th>Queue</th>
          </tr>
        </thead>
        <tbody>
          {items.map((agent) => (
            <tr key={agent.agent_id}>
              <td>
                <strong>{agent.agent_id}</strong>
                <small>{agent.agent_version}</small>
              </td>
              <td>{agent.region}</td>
              <td>{agent.desired_revision}</td>
              <td>
                <Pill tone={validationTone(agent.latest_validation_status)}>
                  {agent.latest_validation_status ?? "unknown"}
                </Pill>
              </td>
              <td>{formatDateTime(agent.latest_heartbeat_accepted_at)}</td>
              <td>
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
        <div className="empty-state compact">No receipts</div>
      ) : (
        <div className="receipt-list">
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
    <div className="receipt-row">
      <div>
        <strong>{receipt.receipt_id ?? "receipt"}</strong>
        <small>{formatDateTime(receipt.accepted_at ?? item.submitted_at)}</small>
      </div>
      <Pill tone={receiptTone(status)}>{String(status)}</Pill>
    </div>
  );
}

function Pill({
  tone,
  children
}: {
  tone: "good" | "warn" | "bad" | "neutral" | "info";
  children: ReactNode;
}) {
  return <span className={`pill ${tone}`}>{children}</span>;
}

function flattenDeploymentOptions(
  grantList: ControlPanelAccessGrantList | null
): DeploymentOption[] {
  if (!grantList) {
    return [];
  }
  return grantList.items.flatMap((grant) =>
    grant.deployment_ids.map((deploymentId) => ({
      customerId: grant.customer_id,
      deploymentId,
      role: grant.role,
      expiresAt: grant.expires_at
    }))
  );
}

function statusTone(status: string): "good" | "warn" | "bad" | "neutral" | "info" {
  if (status === "ready") {
    return "good";
  }
  if (status === "action_required") {
    return "bad";
  }
  if (status === "degraded") {
    return "warn";
  }
  return "neutral";
}

function validationTone(status: string | null): "good" | "warn" | "bad" | "neutral" {
  if (status === "passing") {
    return "good";
  }
  if (status === "degraded") {
    return "warn";
  }
  if (status === "failing") {
    return "bad";
  }
  return "neutral";
}

function receiptTone(status: string): "good" | "warn" | "bad" | "neutral" {
  if (status === "pass" || status === "accepted") {
    return "good";
  }
  if (status === "fail") {
    return "bad";
  }
  if (status === "skipped" || status === "not_submitted") {
    return "warn";
  }
  return "neutral";
}

function priorityTone(priority: string): "good" | "warn" | "bad" | "neutral" {
  if (priority === "critical") {
    return "bad";
  }
  if (priority === "warning") {
    return "warn";
  }
  return "neutral";
}

function roleTone(role: string): "good" | "warn" | "bad" | "neutral" | "info" {
  if (role === "admin") {
    return "bad";
  }
  if (role === "operator") {
    return "info";
  }
  return "neutral";
}

function latestStatus(value: string | null | undefined): string {
  return value ? formatCode(value) : "not observed";
}

function formatCode(value: string): string {
  return value.replace(/_/g, " ");
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return "not observed";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).format(parsed);
}

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : "Unknown BYOC control-panel error";
}
