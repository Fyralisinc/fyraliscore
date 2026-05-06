import type { PageHeader as PageHeaderModel } from "@/api/today-types";

type Props = { header: PageHeaderModel; live?: boolean | null };

// Date headline → first-name greeting → one-line orienting summary.
// The summary is the substrate's first-person framing of the day,
// rendered as a small italic sentence so the user knows what they're
// walking into before they hit the cards.
export function PageHeader({ header, live }: Props) {
  const name = header.viewer_name?.trim();
  const summary = header.state_text?.trim();
  return (
    <div className="page-head">
      <h1 className="page-h1">
        {header.date_label}
        {live === true ? <span className="live-dot">live</span> : null}
        {live === false ? <span className="live-dot off">offline</span> : null}
      </h1>
      <p className="page-greeting">
        {name ? <>Welcome back, <span className="page-greeting-name">{name}</span>.</> : "Welcome back."}
      </p>
      {summary ? <p className="page-summary">{summary}</p> : null}
    </div>
  );
}
