import type { PageHeader as PageHeaderModel } from "@/api/today-types";

type Props = { header: PageHeaderModel; live?: boolean | null };

// Per spec §3.4 — date in serif 32px (always ends in period), state line
// with severity-colored pill + first-person sentence(s). When `live` is
// non-null, show a connection dot that reflects the SSE state.
export function PageHeader({ header, live }: Props) {
  return (
    <div className="page-head">
      <h1 className="page-h1">
        {header.date_label}
        {live === true ? <span className="live-dot">live</span> : null}
        {live === false ? <span className="live-dot off">offline</span> : null}
      </h1>
      <div className="page-head-state">
        <span className={`pill ${header.state_tone}`}>{header.state_tone}</span>
        <span>{header.state_text}</span>
      </div>
    </div>
  );
}
