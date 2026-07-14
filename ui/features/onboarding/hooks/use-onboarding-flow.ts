"use client";

import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";

import { ONBOARDING_SNAPSHOT, ONBOARDING_STEPS } from "../data/mock-data";
import { fetchOnboardingSnapshot } from "../services/onboarding-service";
import { stepIndex, useOnboardingStore } from "../state/onboarding-store";
import type { StepDefinition, StepId } from "../types";

export function useOnboardingSnapshot() {
  return useQuery({
    queryKey: ["onboarding-snapshot"],
    queryFn: fetchOnboardingSnapshot,
    initialData: ONBOARDING_SNAPSHOT
  });
}

export function useOnboardingFlow(currentStepOverride?: StepId) {
  const store = useOnboardingStore();

  return useMemo(() => {
    const effectiveCurrentStep = currentStepOverride ?? store.currentStep;
    const currentIndex = stepIndex(effectiveCurrentStep);
    const completedCount = store.completedSteps.length;
    const completion = (completedCount / ONBOARDING_STEPS.length) * 100;
    const remainingMinutes = ONBOARDING_STEPS.slice(currentIndex + 1).reduce(
      (sum, step) => sum + step.estimateMinutes,
      0
    );
    const currentStep = ONBOARDING_STEPS[currentIndex] ?? ONBOARDING_STEPS[0];

    return {
      ...store,
      steps: ONBOARDING_STEPS.map((step) => ({
        ...step,
        state: stepState(step, effectiveCurrentStep, store.completedSteps)
      })),
      currentStep,
      completion,
      remainingMinutes,
      isCustomerCloud: currentStep.boundary === "customer-cloud"
    };
  }, [currentStepOverride, store]);
}

function stepState(
  step: StepDefinition,
  currentStep: StepId,
  completedSteps: StepId[]
) {
  if (step.id === currentStep) {
    return "current" as const;
  }
  if (completedSteps.includes(step.id)) {
    return "completed" as const;
  }
  const previous = ONBOARDING_STEPS[stepIndex(step.id) - 1]?.id;
  if (!previous || completedSteps.includes(previous)) {
    return "available" as const;
  }
  return "locked" as const;
}
