"""Placeholder smoke test so pytest discovers the package (M1.4)."""


def test_package_importable() -> None:
    import services.ingestion.reconciler  # noqa: F401
