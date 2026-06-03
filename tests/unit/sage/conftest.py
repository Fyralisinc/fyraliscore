"""Shared fixtures for SAGE integration-flavored unit tests.

The SAGE tests use the gateway test database fixtures, but importing
those fixture functions into each test module creates ruff F811 noise
when the same names are used as pytest parameters.
"""

pytest_plugins = ("services.app.gateway.tests.conftest",)
