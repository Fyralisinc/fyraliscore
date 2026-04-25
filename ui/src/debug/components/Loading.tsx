export function Loading({ what = "loading" }: { what?: string }) {
  return <div className="loading">{what}…</div>;
}

export function Empty({ what = "no rows" }: { what?: string }) {
  return <div className="empty">{what}</div>;
}

export function ErrorBox({ message }: { message: string }) {
  return <div className="error-box">error: {message}</div>;
}
