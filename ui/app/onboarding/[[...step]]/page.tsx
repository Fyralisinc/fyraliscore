import { OnboardingApp } from "@/features/onboarding/flows/onboarding-app";
import { ONBOARDING_STEPS } from "@/features/onboarding/data/mock-data";
import type { StepId } from "@/features/onboarding/types";

type PageProps = {
  params: Promise<{
    step?: string[];
  }>;
};

export function generateStaticParams() {
  return ONBOARDING_STEPS.map((step) => ({
    step: [step.id]
  }));
}

export default async function OnboardingPage({ params }: PageProps) {
  const resolved = await params;
  const requested = resolved.step?.[0] as StepId | undefined;
  const step: StepId = requested && ONBOARDING_STEPS.some((item) => item.id === requested)
    ? requested
    : "get-fyralis";

  return <OnboardingApp initialStep={step} />;
}
