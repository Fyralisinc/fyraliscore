"use client";

import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

import { ONBOARDING_STEPS } from "../data/mock-data";
import type {
  CloudReadiness,
  Customer,
  PlanId,
  SourceConnection,
  SourceStatus,
  StepId,
  SyncJob,
  Validation
} from "../types";

type OnboardingStore = {
  selectedPlan: PlanId;
  customer: Customer;
  readiness: CloudReadiness;
  currentStep: StepId;
  completedSteps: StepId[];
  selectedSourceId: string;
  connections: SourceConnection[];
  sourceValidation: Validation;
  syncJobs: SyncJob[];
  launchReady: boolean;
  dirty: boolean;
  lastSavedAt: string | null;
  choosePlan: (plan: PlanId) => void;
  updateCustomer: (customer: Customer) => void;
  updateReadiness: (readiness: CloudReadiness) => void;
  goToStep: (step: StepId) => void;
  completeStep: (step: StepId, next?: StepId) => void;
  selectSource: (sourceId: string) => void;
  updateConnection: (sourceId: string, patch: Partial<SourceConnection>) => void;
  setSourceValidation: (validation: Validation) => void;
  upsertSyncJob: (job: SyncJob) => void;
  setLaunchReady: (ready: boolean) => void;
  markSaved: () => void;
};

const defaultCustomer: Customer = {
  company: "Acme Finance",
  setupOwnerEmail: "platform-owner@acme.example",
  targetCloud: "AWS"
};

const defaultReadiness: CloudReadiness = {
  region: "us-east-1",
  environment: "pilot",
  kubernetes: "available",
  network: "existing-ready",
  secrets: "aws-secrets-manager",
  postgres: "pgvector-ready",
  objectStorage: "s3-compatible-ready",
  kafka: "kafka-msk-ready"
};

const defaultConnections: SourceConnection[] = [
  {
    sourceId: "slack",
    status: "connected",
    selectedScopes: ["#leadership", "#finance-ops", "#customer-success"],
    backfillWindow: "Last 30 days",
    syncMode: "Limited backfill",
    receiptId: "srcval_slack_20260629"
  },
  {
    sourceId: "gmail",
    status: "draft",
    selectedScopes: [],
    backfillWindow: "Last 30 days",
    syncMode: "Limited backfill"
  },
  {
    sourceId: "github",
    status: "ready",
    selectedScopes: [],
    backfillWindow: "Last 30 days",
    syncMode: "Limited backfill"
  }
];

const defaultValidation: Validation = {
  id: "val_source_slack",
  target: "source",
  status: "not-run",
  checks: [
    {
      label: "Secret refs reachable",
      status: "pending",
      detail: "No local source test has run."
    },
    {
      label: "Provider endpoint reachable",
      status: "pending",
      detail: "Waiting for provider callback or local path test."
    },
    {
      label: "Required scopes present",
      status: "pending",
      detail: "Waiting for provider scope check."
    },
    {
      label: "Rate limits acceptable",
      status: "pending",
      detail: "Waiting for pilot batch estimate."
    }
  ]
};

export const useOnboardingStore = create<OnboardingStore>()(
  persist(
    (set, get) => ({
      selectedPlan: "design-partner-byoc",
      customer: defaultCustomer,
      readiness: defaultReadiness,
      currentStep: "get-fyralis",
      completedSteps: [],
      selectedSourceId: "slack",
      connections: defaultConnections,
      sourceValidation: defaultValidation,
      syncJobs: [],
      launchReady: false,
      dirty: false,
      lastSavedAt: null,
      choosePlan: (plan) => set({ selectedPlan: plan, dirty: true }),
      updateCustomer: (customer) => set({ customer, dirty: true }),
      updateReadiness: (readiness) => set({ readiness, dirty: true }),
      goToStep: (step) => {
        if (canNavigateTo(step, get().completedSteps, get().currentStep)) {
          set({ currentStep: step });
        }
      },
      completeStep: (step, next) =>
        set((state) => ({
          completedSteps: Array.from(new Set([...state.completedSteps, step])),
          currentStep: next ?? state.currentStep,
          dirty: true
        })),
      selectSource: (sourceId) =>
        set({
          selectedSourceId: sourceId,
          dirty: true
        }),
      updateConnection: (sourceId, patch) =>
        set((state) => ({
          connections: upsertConnection(state.connections, sourceId, patch),
          dirty: true
        })),
      setSourceValidation: (validation) =>
        set({
          sourceValidation: validation,
          dirty: true
        }),
      upsertSyncJob: (job) =>
        set((state) => ({
          syncJobs: [
            ...state.syncJobs.filter((item) => item.id !== job.id),
            job
          ],
          dirty: true
        })),
      setLaunchReady: (ready) => set({ launchReady: ready, dirty: true }),
      markSaved: () =>
        set({
          dirty: false,
          lastSavedAt: new Date().toISOString()
        })
    }),
    {
      name: "fyralis-onboarding-draft",
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        selectedPlan: state.selectedPlan,
        customer: state.customer,
        readiness: state.readiness,
        completedSteps: state.completedSteps,
        selectedSourceId: state.selectedSourceId,
        connections: state.connections,
        sourceValidation: state.sourceValidation,
        syncJobs: state.syncJobs,
        launchReady: state.launchReady,
        lastSavedAt: state.lastSavedAt
      }),
      merge: (persisted, current) => ({
        ...current,
        ...(persisted as Partial<OnboardingStore>),
        currentStep: current.currentStep,
        dirty: false
      })
    }
  )
);

export function stepIndex(step: StepId) {
  return ONBOARDING_STEPS.findIndex((item) => item.id === step);
}

export function nextStep(step: StepId): StepId {
  const index = stepIndex(step);
  return ONBOARDING_STEPS[Math.min(index + 1, ONBOARDING_STEPS.length - 1)].id;
}

export function sourceStatusLabel(status: SourceStatus) {
  return status
    .split("-")
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

function canNavigateTo(
  target: StepId,
  completedSteps: StepId[],
  currentStep: StepId
) {
  const targetIndex = stepIndex(target);
  const currentIndex = stepIndex(currentStep);
  return (
    targetIndex <= currentIndex ||
    completedSteps.includes(target) ||
    completedSteps.includes(ONBOARDING_STEPS[targetIndex - 1]?.id)
  );
}

function upsertConnection(
  connections: SourceConnection[],
  sourceId: string,
  patch: Partial<SourceConnection>
): SourceConnection[] {
  const existing = connections.find((item) => item.sourceId === sourceId);
  if (!existing) {
    const nextConnection: SourceConnection = {
      sourceId,
      status: "draft",
      selectedScopes: [],
      backfillWindow: "Last 30 days",
      syncMode: "Limited backfill",
      ...patch
    };
    return [
      ...connections,
      nextConnection
    ];
  }
  return connections.map((item) =>
    item.sourceId === sourceId ? { ...item, ...patch } : item
  );
}
