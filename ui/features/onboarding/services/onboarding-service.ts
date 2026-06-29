import { ONBOARDING_SNAPSHOT } from "../data/mock-data";
import type { OnboardingSnapshot } from "../types";

export async function fetchOnboardingSnapshot(): Promise<OnboardingSnapshot> {
  return structuredClone(ONBOARDING_SNAPSHOT);
}
