import { z } from "zod";

export const customerSchema = z.object({
  company: z.string().min(2, "Company name is required."),
  setupOwnerEmail: z.string().email("Use a valid setup owner email."),
  targetCloud: z.enum(["AWS", "GCP future profile", "Azure future profile"])
});

export const cloudReadinessSchema = z.object({
  region: z.enum(["us-east-1", "us-west-2", "eu-west-1"]),
  environment: z.enum(["pilot", "staging", "production"]),
  kubernetes: z.enum(["available", "needs-guidance", "unknown"]),
  network: z.enum(["existing-ready", "needs-isolated-guidance", "unknown"]),
  secrets: z.enum(["aws-secrets-manager", "needs-guidance", "unknown"]),
  postgres: z.enum(["pgvector-ready", "needs-guidance", "unknown"]),
  objectStorage: z.enum(["s3-compatible-ready", "needs-guidance", "unknown"]),
  kafka: z.enum(["kafka-msk-ready", "needs-guidance", "unknown"])
});

export const sourceScopeSchema = z.object({
  selectedScopes: z.array(z.string()).min(1, "Choose at least one scope."),
  backfillWindow: z.enum([
    "Last 7 days",
    "Last 30 days",
    "Last 90 days",
    "No historical backfill"
  ]),
  syncMode: z.enum([
    "Dry run",
    "Limited backfill",
    "Live events",
    "Backfill plus live"
  ])
});

export type CustomerFormValues = z.infer<typeof customerSchema>;
export type CloudReadinessFormValues = z.infer<typeof cloudReadinessSchema>;
export type SourceScopeFormValues = z.infer<typeof sourceScopeSchema>;
