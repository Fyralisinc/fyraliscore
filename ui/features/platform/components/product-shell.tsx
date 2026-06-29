import Link from "next/link";
import type { ReactNode } from "react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

import { HOST_NAV } from "../data/surfaces";

export function ProductShell({
  active,
  eyebrow,
  title,
  description,
  children,
  actions
}: {
  active: string;
  eyebrow: string;
  title: string;
  description: string;
  children: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <main className="min-h-screen bg-background">
      <header className="border-b border-border bg-card px-5 py-4 text-foreground md:px-7">
        <div className="mx-auto flex max-w-7xl flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <Link href="/host/control-panel" className="group inline-flex w-fit items-center gap-3">
            <span className="grid h-10 w-10 place-items-center rounded-lg border border-border bg-primary text-lg font-black text-primary-foreground shadow-panel">
              F
            </span>
            <span>
              <strong className="block text-sm tracking-normal">Fyralis</strong>
              <span className="text-xs font-medium text-muted-foreground">
                Host console
              </span>
            </span>
          </Link>
          <nav className="flex flex-wrap gap-2" aria-label="Primary">
            {HOST_NAV.map((item) => {
              const selected = item.href === active;
              const Icon = item.icon;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={cn(
                    "inline-flex min-h-10 items-center gap-2 rounded-md border px-3 text-sm font-semibold transition-colors",
                    selected
                      ? "border-primary bg-primary text-primary-foreground"
                      : "border-border bg-secondary text-muted-foreground hover:border-ring hover:bg-accent hover:text-accent-foreground"
                  )}
                >
                  <Icon className="h-4 w-4" aria-hidden="true" />
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>
      </header>

      <section className="mx-auto max-w-7xl px-5 py-7 md:px-7">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <Badge tone="info">{eyebrow}</Badge>
            <h1 className="mt-4 max-w-4xl text-balance text-4xl font-semibold tracking-normal md:text-5xl">
              {title}
            </h1>
            <p className="mt-3 max-w-3xl text-base leading-7 text-muted-foreground">
              {description}
            </p>
          </div>
          {actions ? <div className="flex flex-wrap gap-2">{actions}</div> : null}
        </div>
        <div className="mt-7">{children}</div>
      </section>
    </main>
  );
}

export function LinkButton({
  href,
  children,
  variant = "primary",
  className
}: {
  href: string;
  children: ReactNode;
  variant?: "primary" | "secondary" | "ghost";
  className?: string;
}) {
  return (
    <Link
      href={href}
      className={cn(
        "inline-flex min-h-10 items-center justify-center gap-2 rounded-md px-4 text-sm font-semibold transition-colors",
        variant === "primary" &&
          "border border-primary bg-primary text-primary-foreground hover:bg-primary/90",
        variant === "secondary" &&
          "border border-border bg-card text-foreground hover:border-ring hover:bg-accent",
        variant === "ghost" &&
          "border border-transparent bg-transparent text-muted-foreground hover:bg-muted hover:text-foreground",
        className
      )}
    >
      {children}
    </Link>
  );
}
