output "deployment_id" {
  description = "Deployment identifier accepted by this AWS BYOC scaffold."
  value       = var.deployment_id
}

output "customer_id" {
  description = "Customer identifier accepted by this AWS BYOC scaffold."
  value       = var.customer_id
}

output "required_tags" {
  description = "Tags every future customer-owned Fyralis resource must carry."
  value       = local.required_tags
}

output "scaffold_contract" {
  description = "Non-mutating safety contract enforced by the package validator."
  value       = local.scaffold_contract
}

output "expected_role_names" {
  description = "Runtime/IAM role names expected by the permissions manifest."
  value       = local.expected_role_names
}

output "module_contracts" {
  description = "Metadata-only contracts exposed by each non-mutating component module."
  value = {
    iam = module.iam.module_contract
    network = module.network.module_contract
    data_services = module.data_services.module_contract
    runtime = module.runtime.module_contract
    data_plane_agent = module.data_plane_agent.module_contract
  }
}
