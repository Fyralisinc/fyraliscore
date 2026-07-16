from lib.entity_mention_detection import extract_bootstrap_mention_opportunities


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
