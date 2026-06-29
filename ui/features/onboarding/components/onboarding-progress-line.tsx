"use client";

import { Check } from "lucide-react";

import { cn } from "@/lib/utils";

import type { StepDefinition, StepId, StepState } from "../types";

type ProgressStep = StepDefinition & { state: StepState };

const MILESTONES: Array<{
  id: StepId;
  label: string;
}> = [
  { id: "get-fyralis", label: "Get" },
  { id: "cloud-readiness", label: "Cloud" },
  { id: "trust-boundary", label: "Handoff" },
  { id: "deployment", label: "Deploy" },
  { id: "source-catalog", label: "Sources" },
  { id: "workspace-launch", label: "Launch" }
];

export function OnboardingProgressLine({
  steps,
  onStepSelect
}: {
  steps: ProgressStep[];
  onStepSelect: (step: StepId) => void;
}) {
  const currentIndex = steps.findIndex((step) => step.state === "current");

  return (
    <nav
      className="overflow-x-auto border-b border-border bg-background/90 px-5 py-4 md:px-7"
      aria-label="Onboarding milestones"
    >
      <ol className="mx-auto flex min-w-[42rem] max-w-5xl items-center">
        {MILESTONES.map((milestone, index) => {
          const stepIndex = steps.findIndex((step) => step.id === milestone.id);
          const step = steps[stepIndex];
          const reached = stepIndex <= currentIndex;
          const completed = step?.state === "completed" || stepIndex < currentIndex;
          const current = step?.state === "current";
          const disabled = !step || step.state === "locked";

          return (
            <li key={milestone.id} className="flex flex-1 items-center last:flex-none">
              <button
                type="button"
                disabled={disabled}
                onClick={() => onStepSelect(milestone.id)}
                className="group grid min-w-16 justify-items-center gap-2 disabled:pointer-events-none disabled:opacity-45"
              >
                <span
                  className={cn(
                    "grid h-8 w-8 place-items-center rounded-full border text-xs font-semibold transition",
                    completed && "border-success bg-success text-success-foreground",
                    current && "border-success bg-card text-success ring-4 ring-success/15",
                    !reached && "border-border bg-card text-muted-foreground"
                  )}
                >
                  {completed ? <Check className="h-4 w-4" aria-hidden="true" /> : index + 1}
                </span>
                <span
                  className={cn(
                    "text-xs font-semibold",
                    reached ? "text-foreground" : "text-muted-foreground"
                  )}
                >
                  {milestone.label}
                </span>
              </button>
              {index < MILESTONES.length - 1 ? (
                <span
                  className={cn(
                    "mx-2 h-px flex-1 bg-border",
                    completed && "bg-success"
                  )}
                  aria-hidden="true"
                />
              ) : null}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
