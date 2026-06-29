module "iam" {
  source = "./modules/iam"

  deployment_id                 = var.deployment_id
  customer_id                   = var.customer_id
  environment                   = var.environment
  region                        = var.region
  aws_account_id                = var.aws_account_id
  cloudformation_stack_prefix   = var.cloudformation_stack_prefix
  permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
  required_tags                 = local.required_tags
}

module "network" {
  source = "./modules/network"

  deployment_id                 = var.deployment_id
  customer_id                   = var.customer_id
  environment                   = var.environment
  region                        = var.region
  aws_account_id                = var.aws_account_id
  cloudformation_stack_prefix   = var.cloudformation_stack_prefix
  permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
  required_tags                 = local.required_tags
}

module "data_services" {
  source = "./modules/data_services"

  deployment_id                 = var.deployment_id
  customer_id                   = var.customer_id
  environment                   = var.environment
  region                        = var.region
  aws_account_id                = var.aws_account_id
  cloudformation_stack_prefix   = var.cloudformation_stack_prefix
  permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
  required_tags                 = local.required_tags
}

module "runtime" {
  source = "./modules/runtime"

  deployment_id                 = var.deployment_id
  customer_id                   = var.customer_id
  environment                   = var.environment
  region                        = var.region
  aws_account_id                = var.aws_account_id
  cloudformation_stack_prefix   = var.cloudformation_stack_prefix
  permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
  required_tags                 = local.required_tags
}

module "data_plane_agent" {
  source = "./modules/data_plane_agent"

  deployment_id                 = var.deployment_id
  customer_id                   = var.customer_id
  environment                   = var.environment
  region                        = var.region
  aws_account_id                = var.aws_account_id
  cloudformation_stack_prefix   = var.cloudformation_stack_prefix
  permissions_boundary_policy_arn = var.permissions_boundary_policy_arn
  required_tags                 = local.required_tags
}
