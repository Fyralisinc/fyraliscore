variable "deployment_id" {
  description = "Stable Fyralis BYOC deployment identifier from the data-plane manifest."
  type        = string
}

variable "customer_id" {
  description = "Stable Fyralis customer identifier from the data-plane manifest."
  type        = string
}

variable "environment" {
  description = "Deployment environment from the data-plane manifest."
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
