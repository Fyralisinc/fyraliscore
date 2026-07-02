import { OnboardingApp } from "@/features/onboarding/flows/onboarding-app";
import { ONBOARDING_STEPS } from "@/features/onboarding/data/mock-data";
import type { StepId } from "@/features/onboarding/types";

type PageProps = {
  params: Promise<{
    step?: string[];
  }>;
  searchParams: Promise<{
    source?: string | string[];
  }>;
};

const STEP_ALIASES: Record<string, StepId> = {
  sources: "source-catalog",
  integrations: "source-catalog",
  "connect-source": "source-setup",
  "source-connect": "source-setup",
  "source-automated": "ingestion-health"
};

export function generateStaticParams() {
  return [
    ...ONBOARDING_STEPS.map((step) => ({
      step: [step.id]
    })),
    { step: ["sources"] },
    { step: ["sources", "discord"] },
    { step: ["sources", "discord", "automated"] }
  ];
}

export default async function OnboardingPage({ params, searchParams }: PageProps) {
  const resolved = await params;
  const resolvedSearch = await searchParams;
  const requested = resolved.step?.[0];
  const sourceFromPath = requested === "sources" ? resolved.step?.[1] : undefined;
  const sourceFromQuery = Array.isArray(resolvedSearch.source)
    ? resolvedSearch.source[0]
    : resolvedSearch.source;
  const initialSourceId = sourceFromPath ?? sourceFromQuery;
  const step: StepId = resolveOnboardingStep(resolved.step);

  return <OnboardingApp initialStep={step} initialSourceId={initialSourceId} />;
}

function resolveOnboardingStep(parts: string[] | undefined): StepId {
  const requested = parts?.[0];
  if (!requested) {
    return "get-fyralis";
  }
  if (requested === "sources" && parts?.[2] === "automated") {
    return "ingestion-health";
  }
  if (requested === "sources" && parts?.[1]) {
    return "source-setup";
  }
  if (ONBOARDING_STEPS.some((item) => item.id === requested)) {
    return requested as StepId;
  }
  return STEP_ALIASES[requested] ?? "get-fyralis";
}
