variable "deployment_id" {
  description = "Stable Fyralis BYOC deployment identifier."
  type        = string
}

variable "customer_id" {
  description = "Stable Fyralis customer identifier."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
}

variable "region" {
  description = "AWS region for customer-owned data-plane resources."
  type        = string
}

variable "aws_account_id" {
  description = "Customer AWS account identifier that owns the data plane."
  type        = string
}

variable "cloudformation_stack_prefix" {
  description = "Customer-approved stack prefix for Fyralis BYOC resources."
  type        = string
}

variable "permissions_boundary_policy_arn" {
  description = "Customer-owned IAM permissions boundary applied to Fyralis roles."
  type        = string
}

variable "required_tags" {
  description = "Required customer-resource tags supplied by the root module."
  type        = map(string)
}

locals {
  module_contract = {
    component                     = "iam"
    scaffold_status               = "declared"
    deployment_id                 = var.deployment_id
    customer_id                   = var.customer_id
    environment                   = var.environment
    region                        = var.region
    aws_account_id                = var.aws_account_id
    cloudformation_stack_prefix   = var.cloudformation_stack_prefix
    permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
    resource_blocks_declared      = false
    mutating_actions_allowed      = false
    customer_data_inputs_allowed  = false
    sensitive_inputs_allowed      = false
    control_plane_inbound_allowed = false
    required_tag_keys             = sort(keys(var.required_tags))
  }
}

output "module_contract" {
  description = "Metadata-only, non-mutating BYOC component module contract."
  value       = local.module_contract
}
