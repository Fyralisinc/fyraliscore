import { z } from "zod";

export const customerSchema = z.object({
  company: z.string().min(2, "Company name is required."),
  setupOwnerEmail: z.string().email("Use a valid setup owner email."),
  targetCloud: z.enum(["AWS", "GCP future profile", "Azure future profile"])
});

export const cloudReadinessSchema = z.object({
  region: z.enum(["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]),
  environment: z.enum(["pilot", "staging", "production"]),
  setupAutomation: z.enum(["agent-managed"]),
  agentAccess: z.enum(["customer-cloud-agent", "aws-cross-account-role"]),
  agentPermissionProfile: z.enum([
    "byoc-bootstrap-provisioner",
    "discovery-only"
  ]),
  agentApprovalMode: z.enum(["approval-required", "plan-only"]),
  setupRoleArn: z.string().max(220),
  kubernetes: z.enum([
    "available",
    "provision-eks",
    "needs-guidance",
    "unknown"
  ]),
  network: z.enum([
    "existing-ready",
    "provision-isolated-vpc",
    "needs-isolated-guidance",
    "unknown"
  ]),
  secrets: z.enum([
    "aws-secrets-manager",
    "provision-secret-refs",
    "needs-guidance",
    "unknown"
  ]),
  postgres: z.enum([
    "pgvector-ready",
    "provision-rds-pgvector",
    "needs-guidance",
    "unknown"
  ]),
  objectStorage: z.enum([
    "s3-compatible-ready",
    "provision-s3",
    "needs-guidance",
    "unknown"
  ]),
  kafka: z.enum([
    "kafka-msk-ready",
    "provision-msk",
    "needs-guidance",
    "unknown"
  ])
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
