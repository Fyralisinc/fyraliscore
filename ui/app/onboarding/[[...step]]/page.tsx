import { OnboardingApp } from "@/features/onboarding/flows/onboarding-app";
import { ONBOARDING_STEPS } from "@/features/onboarding/data/mock-data";
import type { StepId } from "@/features/onboarding/types";

type PageProps = {
  params: Promise<{
    step?: string[];
  }>;
  searchParams: Promise<{
    source?: string | string[];
    fresh?: string | string[];
  }>;
};

const STEP_ALIASES: Record<string, StepId> = {
  sources: "source-catalog",
  integrations: "source-catalog",
  "connect-source": "source-catalog",
  "source-connect": "source-catalog",
  "source-automated": "source-catalog",
  "first-sync": "source-catalog",
  "ingestion-health": "source-catalog",
  activation: "source-catalog"
};

export function generateStaticParams() {
  return [
    ...ONBOARDING_STEPS.map((step) => ({
      step: [step.id]
    })),
    { step: ["source-setup"] },
    { step: ["source-validation"] },
    { step: ["source-scope"] },
    { step: ["first-sync"] },
    { step: ["ingestion-health"] },
    { step: ["activation"] },
    { step: ["sources"] },
    { step: ["sources", "discord"] },
    { step: ["sources", "discord", "automated"] },
    { step: ["fresh", "discord"] }
  ];
}

export default async function OnboardingPage({ params, searchParams }: PageProps) {
  const resolved = await params;
  const resolvedSearch = await searchParams;
  const requested = resolved.step?.[0];
  const sourceFromPath =
    requested === "sources" || requested === "fresh"
      ? resolved.step?.[1]
      : undefined;
  const sourceFromQuery = Array.isArray(resolvedSearch.source)
    ? resolvedSearch.source[0]
    : resolvedSearch.source;
  const freshParam = Array.isArray(resolvedSearch.fresh)
    ? resolvedSearch.fresh[0]
    : resolvedSearch.fresh;
  const initialSourceId = sourceFromPath ?? sourceFromQuery;
  const step: StepId = resolveOnboardingStep(resolved.step);
  const freshStart = requested === "fresh" || freshParam === "1" || freshParam === "true";

  return (
    <OnboardingApp
      initialStep={step}
      initialSourceId={initialSourceId}
      freshStart={freshStart}
    />
  );
}

function resolveOnboardingStep(parts: string[] | undefined): StepId {
  const requested = parts?.[0];
  if (!requested) {
    return "get-fyralis";
  }
  if (requested === "fresh") {
    return "source-catalog";
  }
  if (requested === "sources" && parts?.[2] === "automated") {
    return "source-catalog";
  }
  if (
    requested === "source-setup" ||
    requested === "source-validation" ||
    requested === "source-scope" ||
    requested === "first-sync" ||
    requested === "ingestion-health" ||
    requested === "activation"
  ) {
    return "source-catalog";
  }
  if (requested === "sources" && parts?.[1]) {
    return "source-catalog";
  }
  if (ONBOARDING_STEPS.some((item) => item.id === requested)) {
    return requested as StepId;
  }
  return STEP_ALIASES[requested] ?? "get-fyralis";
}
