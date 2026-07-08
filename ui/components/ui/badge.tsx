import type { HTMLAttributes } from "react";

import { cn } from "@/lib/utils";

type BadgeTone = "default" | "success" | "warning" | "info" | "muted" | "error";

export function Badge({
  className,
  tone = "default",
  ...props
}: HTMLAttributes<HTMLSpanElement> & { tone?: BadgeTone }) {
  return (
    <span
      className={cn(
        "inline-flex min-h-6 items-center whitespace-nowrap rounded-full border px-2.5 text-xs font-bold",
        tone === "default" && "border-border bg-card text-foreground",
        tone === "success" && "border-success/30 bg-success/10 text-success",
        tone === "warning" && "border-warning/40 bg-warning/15 text-warning-foreground",
        tone === "info" && "border-info/30 bg-info/10 text-info",
        tone === "muted" && "border-border bg-muted text-muted-foreground",
        tone === "error" &&
          "border-destructive/30 bg-destructive/10 text-destructive",
        className
      )}
      {...props}
    />
  );
}
