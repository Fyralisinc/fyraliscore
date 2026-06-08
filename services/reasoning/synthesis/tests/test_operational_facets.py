from __future__ import annotations

from services.reasoning.synthesis.operational_facets import (
    compile_operational_facets,
    enrich_operational_model_proposition,
    infer_operational_query_plan,
)


def test_price_delta_control_becomes_universal_delta_and_state_facets() -> None:
    text = (
        "Form controls visible: radio 500 GB [add $300.00] checked=false; "
        "radio Windows 8 [add $100.00] checked=false; radio Ubuntu checked=true"
    )

    facets = compile_operational_facets(text)

    delta = [facet for facet in facets if facet["role"] == "delta"]
    assert {facet["value"] for facet in delta} == {"500 GB", "Windows 8"}
    assert {facet["attributes"]["amount"] for facet in delta} == {300, 100}
    assert any(
        facet["role"] == "state"
        and facet["value"] == "Ubuntu"
        and facet["state"] == "checked"
        for facet in facets
    )


def test_form_facets_do_not_create_noisy_parser_artifacts() -> None:
    text = (
        "Form controls visible: radio 500 GB [add $300.00] checked=false; "
        "radio Windows 8 [add $100.00] checked=false; "
        "'Quantity' value='2'; 'Catalog item' value='Development Laptop (PC)'"
    )

    facets = compile_operational_facets(text)

    state_values = {
        facet["value"]
        for facet in facets
        if facet["role"] == "state" and facet["subrole"] == "choice_state"
    }
    assert state_values == {"500 GB", "Windows 8"}

    field_values = [
        facet for facet in facets
        if facet["role"] == "property" and facet["subrole"] == "field_value"
    ]
    assert {
        (facet["property"], facet["value"])
        for facet in field_values
    } == {
        ("Quantity", "2"),
        ("Catalog item", "Development Laptop (PC)"),
    }
    assert all(
        facet["property"] not in {"checked", "selected", "value"}
        for facet in field_values
    )


def test_list_order_and_bottom_option_are_sequence_and_state_facets() -> None:
    text = (
        "Relevant structured UI facts: field list Users option order: "
        "for text; User ID; Name; Email; Business phone; "
        "bottom_option = Business phone"
    )

    facets = compile_operational_facets(text)

    assert any(
        facet["role"] == "sequence"
        and facet["subrole"] == "list_option_order"
        and facet["subject"] == "Users"
        for facet in facets
    )
    assert any(
        facet["role"] == "state"
        and facet["property"] == "bottom_option"
        and facet["value"] == "Business phone"
        for facet in facets
    )


def test_stage_chain_and_counts_are_sequence_and_count_facets() -> None:
    text = (
        "After action pipeline stage chains: Waiting for Approval (In progress); "
        "Fulfillment (Pending - has not started); Completed (Pending - has not started); "
        "remaining_excluding_in_progress_count=2"
    )

    facets = compile_operational_facets(text)

    assert any(
        facet["role"] == "sequence"
        and facet["subrole"] == "stage_chain"
        and "Fulfillment" in facet["value"]
        for facet in facets
    )
    assert any(
        facet["role"] == "count"
        and facet["attributes"]["count"] == 2
        for facet in facets
    )


def test_enrichment_preserves_stance_and_adds_operational_roles() -> None:
    proposition = {
        "kind": "observation",
        "summary": "Observed catalog controls.",
    }

    enriched = enrich_operational_model_proposition(
        proposition,
        natural="checkbox Adobe Photoshop checked=false; option 1 selected=true",
    )

    assert enriched["kind"] == "observation"
    assert enriched["operational_facet_schema"] == "operational_facets_v1"
    assert "state" in enriched["operational_roles"]
    assert "operations" in enriched["domain_tags"]


def test_composite_situations_do_not_get_atomic_operational_facet_indexes() -> None:
    proposition = {
        "kind": "belief",
        "claim_role": "situation",
        "abstraction_level": "composite",
        "summary": "Observed catalog form state.",
    }

    enriched = enrich_operational_model_proposition(
        proposition,
        natural=(
            "Composite operational situation: radio 500 GB [add $300.00] "
            "checked=false; 'Quantity' value='2'"
        ),
    )

    assert enriched == proposition


def test_plain_prose_does_not_get_pretend_structure() -> None:
    text = "Alex said the rollout felt healthier after the planning session."

    assert compile_operational_facets(text) == ()


def test_query_plan_uses_universal_roles_not_domain_objects() -> None:
    plan = infer_operational_query_plan(
        "When we order a laptop, what is the extra dollar amount for the largest option?"
    )

    assert "delta" in plan.roles
    assert "value" in plan.roles
    assert "laptop" in plan.terms
    assert "dell_xps" not in plan.roles


def test_query_plan_does_not_treat_largest_as_list_ordering() -> None:
    plan = infer_operational_query_plan(
        "When we order a Dell XPS as the developer laptop, what is the "
        "extra dollar amount if we choose the largest SSD option?"
    )

    assert "delta" in plan.roles
    assert "state" in plan.roles
    assert "sequence" not in plan.roles
    assert "dell" in plan.terms
    assert "ssd" in plan.terms


def test_query_plan_distinguishes_purchase_order_from_sort_order() -> None:
    purchase = infer_operational_query_plan(
        "Order a hardware item with the required configuration."
    )
    sorting = infer_operational_query_plan(
        "Sort the asset list by purchase date order by newest first."
    )

    assert "sequence" not in purchase.roles
    assert "sequence" in sorting.roles
