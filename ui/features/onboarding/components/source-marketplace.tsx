"use client";

import { Search, SlidersHorizontal } from "lucide-react";
import { useMemo, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/form";
import { cn } from "@/lib/utils";

import { sourceStatusLabel } from "../state/onboarding-store";
import type { Source, SourceCategory, SourceConnection } from "../types";

const CATEGORY_OPTIONS: Array<SourceCategory | "All"> = [
  "All",
  "Communication",
  "Engineering",
  "Productivity",
  "Knowledge",
  "CRM",
  "Meetings",
  "Finance",
  "People",
  "Cloud",
  "Design",
  "Operations"
];

export function SourceMarketplace({
  sources,
  connections,
  selectedSourceId,
  onSelect,
  onOpenSetup
}: {
  sources: Source[];
  connections: SourceConnection[];
  selectedSourceId: string;
  onSelect: (sourceId: string) => void;
  onOpenSetup: (sourceId: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState<SourceCategory | "All">("All");

  const filtered = useMemo(
    () =>
      sources.filter((source) => {
        const matchesQuery =
          source.name.toLowerCase().includes(query.toLowerCase()) ||
          source.description.toLowerCase().includes(query.toLowerCase()) ||
          source.requiredPermissions.some((permission) =>
            permission.toLowerCase().includes(query.toLowerCase())
          );
        const matchesCategory =
          category === "All" || source.category === category;
        return matchesQuery && matchesCategory;
      }),
    [category, query, sources]
  );

  return (
    <div className="grid min-w-0 gap-5">
      <div className="flex min-w-0 flex-col gap-3 rounded-lg border border-border bg-card p-4 md:flex-row md:items-center">
        <label className="relative min-w-0 flex-1 md:max-w-sm">
          <span className="sr-only">Search integrations</span>
          <Search
            className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search Slack, GitHub, Google, finance, people..."
            className="pl-9"
          />
        </label>
        <div className="flex min-w-0 flex-1 items-center gap-2 overflow-x-auto pb-1 md:pb-0">
          <SlidersHorizontal
            className="h-4 w-4 shrink-0 text-muted-foreground"
            aria-hidden="true"
          />
          {CATEGORY_OPTIONS.map((item) => (
            <Button
              key={item}
              type="button"
              variant={category === item ? "primary" : "secondary"}
              className="min-h-9 shrink-0 px-3"
              onClick={() => setCategory(item)}
            >
              {item}
            </Button>
          ))}
        </div>
      </div>

      {filtered.length ? (
        <div className="grid min-w-0 gap-3 md:grid-cols-2 xl:grid-cols-3">
          {filtered.map((source) => {
            const connection = connections.find(
              (item) => item.sourceId === source.id
            );
            const status = connection?.status ?? "not-configured";
            const selected = selectedSourceId === source.id;

            return (
              <Card
                key={source.id}
                className={cn(
                  "group transition-colors hover:border-ring",
                  selected && "border-success ring-2 ring-success/15"
                )}
              >
                <CardContent className="grid min-h-64 content-between gap-4 p-4">
                  <button
                    type="button"
                    className="text-left"
                    onClick={() => onSelect(source.id)}
                  >
                    <span className="flex items-start justify-between gap-3">
                      <span>
                        <strong className="block text-lg">{source.name}</strong>
                        <span className="mt-1 block text-sm leading-6 text-muted-foreground">
                          {source.description}
                        </span>
                      </span>
                      <Badge tone={statusTone(status)}>
                        {sourceStatusLabel(status)}
                      </Badge>
                    </span>
                    <span className="mt-4 flex flex-wrap gap-2">
                      <Badge tone="muted">{source.category}</Badge>
                      <Badge tone="info">{source.method}</Badge>
                    </span>
                  </button>

                  <div className="border-t border-border pt-4">
                    <p className="line-clamp-2 text-xs font-medium leading-5 text-muted-foreground">
                      {source.setupRequirements}
                    </p>
                    <Button
                      type="button"
                      className="mt-4 w-full"
                      variant={status === "connected" ? "secondary" : "primary"}
                      onClick={() => onOpenSetup(source.id)}
                    >
                      {status === "connected" ? "View setup" : "Open setup"}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      ) : (
        <Card>
          <CardContent className="p-8 text-center">
            <strong>No integrations match that search.</strong>
            <p className="mt-2 text-sm text-muted-foreground">
              Clear the search or switch category filters to continue.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function statusTone(status: SourceConnection["status"]) {
  if (status === "connected" || status === "ready") {
    return "success" as const;
  }
  if (status === "validating" || status === "draft") {
    return "info" as const;
  }
  if (status === "error") {
    return "error" as const;
  }
  return "muted" as const;
}
