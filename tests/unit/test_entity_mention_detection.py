from lib.entity_mention_detection import (
    extract_bootstrap_mention_opportunities,
    locate_explicit_surface_spans,
)


def test_bootstrap_opportunities_are_maximal_source_surfaces() -> None:
    text = (
        "Project Northstar met NBI about frobozz-widget; "
        "the project is blocked."
    )

    assert extract_bootstrap_mention_opportunities(text) == (
        "Project Northstar",
        "NBI",
        "frobozz-widget",
        "the project",
    )


def test_bootstrap_opportunities_are_bounded_and_deduplicated() -> None:
    text = " and ".join(["NBI", "Northstar", "NBI", "Fyralis"])

    assert extract_bootstrap_mention_opportunities(
        text,
        max_opportunities=2,
    ) == ("NBI", "Northstar")


def test_slack_native_references_and_overlaps_preserve_exact_maximal_surface() -> None:
    text = (
        "The Project owner <@U01ALICE|Alice> posted in <#C01ENG|eng>; "
        "ask <@U01ALICE|Alice>."
    )

    assert extract_bootstrap_mention_opportunities(text) == (
        "The Project",
        "<@U01ALICE|Alice>",
        "<#C01ENG|eng>",
    )


def test_bootstrap_opportunities_ignore_unbounded_lowercase_prose() -> None:
    assert extract_bootstrap_mention_opportunities(
        "we should probably revisit this tomorrow"
    ) == ()


def test_dotted_names_acronyms_possessives_and_plurals_keep_source_surface() -> None:
    text = (
        "Atlas.Pay met N.B.I. after Acme's review; "
        "Horizons and Platform's owners replied."
    )

    assert extract_bootstrap_mention_opportunities(text) == (
        "Atlas.Pay",
        "N.B.I.",
        "Acme's",
        "Horizons",
        "Platform's",
    )


def test_dotted_and_possessive_support_does_not_harvest_lowercase_prose() -> None:
    text = (
        "example.com and v1.2.3 are references; "
        "it's a team's ordinary prose."
    )

    assert extract_bootstrap_mention_opportunities(text) == ()


def test_extended_source_surfaces_remain_bounded_and_deduplicated() -> None:
    text = "Atlas.Pay and Atlas.Pay and N.B.I. and Acme's"

    assert extract_bootstrap_mention_opportunities(
        text,
        max_opportunities=2,
    ) == ("Atlas.Pay", "N.B.I.")


def test_unicode_names_preserve_exact_composed_decomposed_and_full_width_surfaces() -> None:
    surfaces = (
        "Café Ops",
        "Cafe\u0301 Ops",
        "Ａtlas-Gateway",
    )

    for surface in surfaces:
        assert extract_bootstrap_mention_opportunities(surface) == (surface,)


def test_unicode_names_remain_bounded_and_deduplicated() -> None:
    text = (
        "Café Ops and Café Ops and "
        "Cafe\u0301 Ops and Cafe\u0301 Ops and "
        "Ａtlas-Gateway"
    )

    assert extract_bootstrap_mention_opportunities(
        text,
        max_opportunities=2,
    ) == ("Café Ops", "Cafe\u0301 Ops")


def test_unicode_support_does_not_harvest_lowercase_prose() -> None:
    assert extract_bootstrap_mention_opportunities(
        "café ops discussed naïve routing with the résumé owner"
    ) == ()


def test_explicit_company_identifiers_are_kept_without_harvesting_versions() -> None:
    text = "ENG-482 blocks OPS_17, but v1.2.3 and 2026-07-17 are metadata."

    assert extract_bootstrap_mention_opportunities(text) == (
        "ENG-482",
        "OPS_17",
    )


def test_entity_cued_quoted_lowercase_names_preserve_inner_source_span() -> None:
    text = (
        "the project “phoenix gateway” depends on service 'billing-v2'; "
        'someone merely said "ship tomorrow".'
    )

    assert extract_bootstrap_mention_opportunities(text) == (
        "the project",
        "phoenix gateway",
        "billing-v2",
    )


def test_name_particles_join_names_but_conjunctions_keep_entities_separate() -> None:
    text = "Bank of America and Research and Development met Northstar."

    assert extract_bootstrap_mention_opportunities(text) == (
        "Bank of America",
        "Research",
        "Development",
        "Northstar",
    )


def test_slack_user_channel_and_user_group_markup_remain_exact_and_context_tokens_do_not() -> None:
    text = (
        "<@U01ALICE|Alice> asked <#C01OPS|ops> and "
        "<!subteam^S01ONCALL|@on-call>; @bob.smith replied in #rev-ops, "
        "while <!here> should check it."
    )

    assert extract_bootstrap_mention_opportunities(text) == (
        "<@U01ALICE|Alice>",
        "<#C01OPS|ops>",
        "<!subteam^S01ONCALL|@on-call>",
        "bob.smith",
        "#rev-ops",
    )


def test_sentence_pronouns_temporal_words_and_generic_statuses_are_not_names() -> None:
    text = "He Said Update. Monday Project Blocked. Thanks."

    assert extract_bootstrap_mention_opportunities(text) == ()


def test_locator_preserves_repeated_unicode_and_possessive_source_coordinates() -> None:
    text = "Café Ops met CAFE OPS; Acme's update followed Acme's review."

    assert locate_explicit_surface_spans(text, "café ops") == ((0, 8),)
    assert locate_explicit_surface_spans(text, "Acme's") == ((23, 29), (46, 52))


def test_identifiers_roles_and_people_are_separate_maximal_surfaces() -> None:
    text = "Decision D-17 moved from VP Sales Jordan Lee to Team Aurora."

    assert extract_bootstrap_mention_opportunities(text) == (
        "D-17",
        "VP Sales",
        "Jordan Lee",
        "Team Aurora",
    )


def test_ampersand_names_and_modified_definite_references_keep_boundaries() -> None:
    text = "M&A Readiness alerted The API team and Legal Ops."

    assert extract_bootstrap_mention_opportunities(text) == (
        "M&A Readiness",
        "The API team",
        "Legal Ops",
    )


def test_generic_capitalized_metadata_does_not_create_mentions() -> None:
    text = "Routine update Friday. Quoted from Jira. No blockers."

    assert extract_bootstrap_mention_opportunities(text) == ()
