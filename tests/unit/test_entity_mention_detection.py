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
