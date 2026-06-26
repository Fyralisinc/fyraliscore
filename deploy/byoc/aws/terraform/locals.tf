locals {
  required_tags = {
    "fyralis:deployment-id" = var.deployment_id
    "fyralis:customer-id"   = var.customer_id
    "fyralis:managed"       = "true"
    "fyralis:environment"   = var.environment
  }

  scaffold_contract = {
    package_status                         = "scaffold_only"
    customer_side_bootstrap_required       = true
    terraform_apply_allowed                = false
    control_plane_mutating_access_allowed  = false
    stores_remote_state_in_control_plane   = false
    no_inbound_control_plane_ports         = true
    outbound_control_plane_port            = 443
  }

  expected_role_names = [
    "bootstrap_provisioner",
    "cloudformation_service",
    "data_plane_agent",
    "gateway_runtime",
    "worker_runtime",
    "migration_runner",
    "observability_runtime",
  ]
}
