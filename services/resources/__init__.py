"""services/resources — Resources aggregate (Wave 2-C).

Owns: resources, resource_transactions (partitioned monthly by
occurred_at), resource_deployments, customer_commitments, and the
Bridge *primitives* (revenue_at_risk, capability_at_risk,
feasibility_check). Full Bridge queries and dashboards are Wave 5-B.

Per SCHEMA-QUESTION.md Q2 the §27 superset columns of
customer_commitments (revenue_at_risk_usd, relationship_kind,
criticality) are NOT referenced here. We use the §4 shape
(customer_resource_id, commitment_id, served_description) with the
composite PK (customer_resource_id, commitment_id).
"""
