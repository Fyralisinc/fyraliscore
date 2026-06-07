"""services/resources — Resources aggregate.

Owns: resources, resource_transactions (partitioned monthly by
occurred_at), resource_deployments, and customer_commitments.

Per SCHEMA-QUESTION.md Q2 the §27 superset columns of
customer_commitments (revenue_at_risk_usd, relationship_kind,
criticality) are treated as relationship metadata. Core resource code uses
the customer_resource_id / commitment_id relationship with the composite
PK (customer_resource_id, commitment_id).
"""
