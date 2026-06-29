"use client";

import { AnimatePresence, motion } from "framer-motion";
import { AlertTriangle, Loader2 } from "lucide-react";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useMemo } from "react";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

import { OnboardingProgressLine } from "../components/onboarding-progress-line";
import { ONBOARDING_STEPS } from "../data/mock-data";
import { useOnboardingFlow, useOnboardingSnapshot } from "../hooks/use-onboarding-flow";
import {
  nextStep,
  stepIndex,
  useOnboardingStore
} from "../state/onboarding-store";
import type { SourceConnection, StepId } from "../types";
import { StepView, type StepViewProps } from "./step-views";

export function OnboardingApp({ initialStep }: { initialStep: StepId }) {
  const router = useRouter();
  const pathname = usePathname();
  const snapshotQuery = useOnboardingSnapshot();
  const flow = useOnboardingFlow();
  const store = useOnboardingStore();
  const routeStep = useMemo(() => {
    const candidate = pathname.split("/").filter(Boolean).at(-1) as
      | StepId
      | undefined;
    return ONBOARDING_STEPS.some((step) => step.id === candidate)
      ? candidate
      : initialStep;
  }, [initialStep, pathname]);

  useEffect(() => {
    useOnboardingStore.setState({ currentStep: routeStep });
  }, [routeStep]);

  useEffect(() => {
    if (!flow.dirty) {
      return;
    }
    const id = window.setTimeout(() => {
      store.markSaved();
    }, 900);
    return () => window.clearTimeout(id);
  }, [flow.dirty, store]);

  useEffect(() => {
    const listener = (event: BeforeUnloadEvent) => {
      if (!useOnboardingStore.getState().dirty) {
        return;
      }
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", listener);
    return () => window.removeEventListener("beforeunload", listener);
  }, []);

  useEffect(() => {
    const listener = (event: KeyboardEvent) => {
      if (!event.altKey) {
        return;
      }
      if (event.key === "ArrowRight") {
        event.preventDefault();
        advance();
      }
      if (event.key === "ArrowLeft") {
        event.preventDefault();
        const index = Math.max(0, stepIndex(useOnboardingStore.getState().currentStep) - 1);
        goTo(ONBOARDING_STEPS[index].id);
      }
    };
    window.addEventListener("keydown", listener);
    return () => window.removeEventListener("keydown", listener);
  });

  const snapshot = snapshotQuery.data;

  const selectedSource = useMemo(() => {
    const sources = snapshot?.sources ?? [];
    return (
      sources.find((source) => source.id === flow.selectedSourceId) ??
      sources[0]
    );
  }, [flow.selectedSourceId, snapshot?.sources]);

  const selectedConnection = useMemo<SourceConnection | undefined>(
    () =>
      selectedSource
        ? flow.connections.find((item) => item.sourceId === selectedSource.id)
        : undefined,
    [flow.connections, selectedSource]
  );

  function pushStep(step: StepId) {
    router.push(`/onboarding/${step}`);
  }

  function goTo(step: StepId) {
    store.goToStep(step);
    pushStep(step);
  }

  function advance() {
    const current = useOnboardingStore.getState().currentStep;
    const next = nextStep(current);
    store.completeStep(current, next);
    pushStep(next);
  }

  if (snapshotQuery.isError) {
    return (
      <ErrorState
        message={
          snapshotQuery.error instanceof Error
            ? snapshotQuery.error.message
            : "Could not load onboarding."
        }
        onRetry={() => void snapshotQuery.refetch()}
      />
    );
  }

  if (snapshotQuery.isLoading || !snapshot) {
    return <LoadingState />;
  }

  if (!selectedSource) {
    return (
      <ErrorState
        message="No source catalog is available for this onboarding workspace."
        onRetry={() => void snapshotQuery.refetch()}
      />
    );
  }

  const props: StepViewProps = {
    snapshot,
    selectedPlan: flow.selectedPlan,
    customer: flow.customer,
    readiness: flow.readiness,
    workspace: snapshot.workspace,
    selectedSource,
    selectedConnection,
    connections: flow.connections,
    sourceValidation: flow.sourceValidation,
    syncJobs: flow.syncJobs,
    launchReady: flow.launchReady,
    choosePlan: store.choosePlan,
    updateCustomer: store.updateCustomer,
    updateReadiness: store.updateReadiness,
    selectSource: store.selectSource,
    updateConnection: store.updateConnection,
    setSourceValidation: store.setSourceValidation,
    upsertSyncJob: store.upsertSyncJob,
    setLaunchReady: store.setLaunchReady,
    goTo,
    advance
  };

  return (
    <main className="min-h-screen bg-background">
      <OnboardingProgressLine steps={flow.steps} onStepSelect={goTo} />

      <section className="mx-auto max-w-6xl px-5 py-6 md:px-7">
        <AnimatePresence mode="wait">
          <motion.div
            key={flow.currentStep.id}
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.18, ease: "easeOut" }}
          >
            <StepView stepId={flow.currentStep.id} props={props} />
          </motion.div>
        </AnimatePresence>
      </section>
    </main>
  );
}

function LoadingState() {
  return (
    <main className="grid min-h-screen place-items-center p-6">
      <Card className="w-full max-w-md">
        <CardContent className="flex items-center gap-3 p-6">
          <Loader2 className="h-5 w-5 animate-spin text-info" aria-hidden="true" />
          <span>
            <strong className="block">Loading onboarding workspace</strong>
            <span className="text-sm text-muted-foreground">
              Restoring draft, step progress, and source catalog.
            </span>
          </span>
        </CardContent>
      </Card>
    </main>
  );
}

function ErrorState({
  message,
  onRetry
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <main className="grid min-h-screen place-items-center p-6">
      <Card className="w-full max-w-lg border-destructive/30">
        <CardContent className="grid gap-4 p-6">
          <div className="flex gap-3">
            <AlertTriangle
              className="mt-1 h-5 w-5 text-destructive"
              aria-hidden="true"
            />
            <span>
              <strong className="block">Onboarding could not load</strong>
              <span className="text-sm text-muted-foreground">{message}</span>
            </span>
          </div>
          <Button type="button" onClick={onRetry}>
            Retry
          </Button>
        </CardContent>
      </Card>
    </main>
  );
}
