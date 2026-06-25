"""Tests for lib/observability/metrics.py — families, registry, render.

The default registry is process-global, so tests against it use unique
metric names and assert on specific labeled lines (never whole-output
equality). Family-level behavior is tested on fresh Registry instances.
"""
from __future__ import annotations

import pytest

from lib.observability.metrics import (
    Counter,
    Histogram,
    Registry,
    counter,
    gauge,
    histogram,
    render_default,
    reset_default_for_tests,
)


@pytest.fixture(autouse=True)
def _clean_default_registry():
    reset_default_for_tests()
    yield
    reset_default_for_tests()


# ---------------------------------------------------------------------------
# Counter
# ---------------------------------------------------------------------------


class TestCounter:
    def test_inc_and_get(self) -> None:
        c = Counter("obs_t_requests_total", "Requests.", ("a",))
        assert c.get(a="x") == 0.0
        c.inc(a="x")
        c.inc(a="x")
        c.inc(5, a="y")
        assert c.get(a="x") == 2.0
        assert c.get(a="y") == 5.0

    def test_wrong_label_set_raises(self) -> None:
        c = Counter("obs_t_labeled_total", "Help.", ("a",))
        with pytest.raises(ValueError):
            c.inc(b="x")
        with pytest.raises(ValueError):
            c.inc()  # missing the required label
        with pytest.raises(ValueError):
            c.inc(a="x", extra="y")
        with pytest.raises(ValueError):
            c.get(b="x")

    def test_negative_inc_raises(self) -> None:
        c = Counter("obs_t_updown_total", "Help.")
        with pytest.raises(ValueError):
            c.inc(-1)

    def test_render_has_help_type_and_labeled_line(self) -> None:
        c = Counter("name", "Things counted.", ("a",))
        c.inc(a="x")
        c.inc(a="x")
        lines = c.render()
        assert "# HELP name Things counted." in lines
        assert "# TYPE name counter" in lines
        assert 'name{a="x"} 2' in lines

    def test_label_value_escaping(self) -> None:
        c = Counter("obs_t_escape_total", "Help.", ("l",))
        c.inc(l='a"b\\c')
        text = "\n".join(c.render())
        # backslash -> \\, quote -> \"
        assert 'obs_t_escape_total{l="a\\"b\\\\c"} 1' in text
        for line in c.render():
            assert "\n" not in line

    @pytest.mark.parametrize(
        "label_name",
        [
            "tenant_id",
            "actor_id",
            "installation_id",
            "query",
            "object_key",
            "payload",
            "prompt",
            "source_channel",
        ],
    )
    def test_forbidden_label_names_raise(self, label_name: str) -> None:
        with pytest.raises(ValueError, match="forbidden"):
            Counter("obs_t_forbidden_total", "Help.", (label_name,))

    @pytest.mark.parametrize(
        "label_value",
        [
            "4c669853-a589-48ba-b80e-7ad60eb05f5b",
            "alice@example.com",
            "https://example.com/raw",
            "/v1/models/4c669853-a589-48ba-b80e-7ad60eb05f5b",
            "Bearer sk-test",
            "line\nbreak",
        ],
    )
    def test_unsafe_label_values_raise(self, label_value: str) -> None:
        c = Counter("obs_t_unsafe_label_total", "Help.", ("route",))
        with pytest.raises(ValueError):
            c.inc(route=label_value)

    def test_allowed_label_values_reject_free_form_values(self) -> None:
        c = Counter(
            "obs_t_allowlisted_total",
            "Help.",
            ("component", "outcome"),
            allowed_label_values={
                "component": ("gateway", "normalizer"),
                "outcome": ("success", "failure"),
            },
        )

        c.inc(component="gateway", outcome="success")
        assert c.get(component="gateway", outcome="success") == 1
        with pytest.raises(ValueError, match="allowlist"):
            c.inc(component="gateway", outcome="raw-customer-channel")

    def test_allowlist_contract_is_part_of_reregistration_identity(self) -> None:
        reg = Registry()
        reg.counter(
            "obs_t_contract_total",
            "Help.",
            ("outcome",),
            allowed_label_values={"outcome": ("ok", "error")},
        )
        with pytest.raises(ValueError, match="allowlist"):
            reg.counter(
                "obs_t_contract_total",
                "Help.",
                ("outcome",),
                allowed_label_values={"outcome": ("ok", "error", "skipped")},
            )

    def test_allowlist_rejects_unknown_label_declaration(self) -> None:
        with pytest.raises(ValueError, match="unknown metric label"):
            Counter(
                "obs_t_bad_allowlist_total",
                "Help.",
                ("outcome",),
                allowed_label_values={"missing": ("ok",)},
            )

    @pytest.mark.parametrize("label_name", ["channel", "channel_name", "source_channel"])
    def test_channel_label_names_are_forbidden(self, label_name: str) -> None:
        with pytest.raises(ValueError, match="forbidden"):
            Counter("obs_t_channel_forbidden_total", "Help.", (label_name,))


# ---------------------------------------------------------------------------
# Gauge (via the default-registry helper)
# ---------------------------------------------------------------------------


class TestGauge:
    def test_set_inc_get_and_render(self) -> None:
        g = gauge("obs_t_temperature", "A gauge.", ("room",))
        g.set(3.0, room="lab")
        g.inc(2.0, room="lab")
        assert g.get(room="lab") == 5.0
        text = render_default()
        assert 'obs_t_temperature{room="lab"} 5' in text
        assert "# TYPE obs_t_temperature gauge" in text


# ---------------------------------------------------------------------------
# Histogram
# ---------------------------------------------------------------------------


class TestHistogram:
    BUCKETS = (0.1, 1.0, 10.0)

    def test_cumulative_buckets_inf_sum_count(self) -> None:
        h = Histogram("obs_t_lat_seconds", "Latency.", ("op",), buckets=self.BUCKETS)
        for v in (0.05, 0.5, 0.5, 100.0):
            h.observe(v, op="read")
        lines = h.render()
        assert 'obs_t_lat_seconds_bucket{op="read",le="0.1"} 1' in lines
        assert 'obs_t_lat_seconds_bucket{op="read",le="1"} 3' in lines
        assert 'obs_t_lat_seconds_bucket{op="read",le="10"} 3' in lines
        # +Inf bucket always equals the total count
        assert 'obs_t_lat_seconds_bucket{op="read",le="+Inf"} 4' in lines
        assert 'obs_t_lat_seconds_count{op="read"} 4' in lines
        assert h.get_count(op="read") == 4
        assert h.get_sum(op="read") == pytest.approx(101.05)
        sum_lines = [
            ln for ln in lines if ln.startswith('obs_t_lat_seconds_sum{op="read"}')
        ]
        assert len(sum_lines) == 1
        assert float(sum_lines[0].split()[-1]) == pytest.approx(101.05)

    def test_value_above_top_bucket_lands_only_in_inf(self) -> None:
        h = Histogram("obs_t_big_seconds", "Help.", buckets=self.BUCKETS)
        h.observe(100.0)
        lines = h.render()
        assert 'obs_t_big_seconds_bucket{le="0.1"} 0' in lines
        assert 'obs_t_big_seconds_bucket{le="1"} 0' in lines
        assert 'obs_t_big_seconds_bucket{le="10"} 0' in lines
        assert 'obs_t_big_seconds_bucket{le="+Inf"} 1' in lines
        assert h.get_count() == 1

    def test_render_order_buckets_then_inf_sum_count(self) -> None:
        h = Histogram("obs_t_order_seconds", "Help.", buckets=self.BUCKETS)
        h.observe(0.5)
        lines = h.render()
        idx = {
            "help": lines.index("# HELP obs_t_order_seconds Help."),
            "type": lines.index("# TYPE obs_t_order_seconds histogram"),
            "b01": lines.index('obs_t_order_seconds_bucket{le="0.1"} 0'),
            "b1": lines.index('obs_t_order_seconds_bucket{le="1"} 1'),
            "b10": lines.index('obs_t_order_seconds_bucket{le="10"} 1'),
            "inf": lines.index('obs_t_order_seconds_bucket{le="+Inf"} 1'),
            "sum": lines.index("obs_t_order_seconds_sum 0.5"),
            "count": lines.index("obs_t_order_seconds_count 1"),
        }
        order = ["help", "type", "b01", "b1", "b10", "inf", "sum", "count"]
        positions = [idx[k] for k in order]
        assert positions == sorted(positions)

    def test_empty_buckets_rejected(self) -> None:
        with pytest.raises(ValueError):
            Histogram("obs_t_nobuckets", "Help.", buckets=())


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_get_or_create_returns_same_object(self) -> None:
        reg = Registry()
        a = reg.counter("dup_total", "Help.", ("a",))
        b = reg.counter("dup_total", "Help.", ("a",))
        assert a is b
        # default-registry helpers go through the same path
        c1 = counter("obs_t_helper_total", "Help.", ("a",))
        c2 = counter("obs_t_helper_total", "Help.", ("a",))
        assert c1 is c2

    def test_reregister_with_different_labels_raises(self) -> None:
        reg = Registry()
        reg.counter("clash_total", "Help.", ("a",))
        with pytest.raises(ValueError):
            reg.counter("clash_total", "Help.", ("a", "b"))

    def test_reregister_with_different_type_raises(self) -> None:
        reg = Registry()
        reg.counter("typed_total", "Help.", ("a",))
        with pytest.raises(ValueError):
            reg.gauge("typed_total", "Help.", ("a",))

    def test_empty_families_omitted_from_render(self) -> None:
        reg = Registry()
        reg.counter("never_used_total", "Help.", ("a",))
        used = reg.counter("used_total", "Help.")
        used.inc()
        text = reg.render_text()
        assert "never_used_total" not in text
        assert "used_total 1" in text

    def test_raising_collector_is_skipped(self) -> None:
        reg = Registry()

        def bad() -> str:
            raise RuntimeError("mid-shutdown")

        def good() -> str:
            return "collected_metric 7\n"

        reg.add_collector(bad)
        reg.add_collector(good)
        text = reg.render_text()  # must not raise
        assert "collected_metric 7" in text

    def test_default_registry_collector_skipped_when_raising(self) -> None:
        from lib.observability.metrics import default_registry

        def bad() -> str:
            raise RuntimeError("boom")

        default_registry().add_collector(bad)
        try:
            text = render_default()  # must not raise despite the bad collector
            assert isinstance(text, str)
        finally:
            default_registry().remove_collector(bad)

    def test_reset_for_tests_zeroes_counters(self) -> None:
        c = counter("obs_t_reset_total", "Help.", ("a",))
        c.inc(a="x")
        assert 'obs_t_reset_total{a="x"} 1' in render_default()
        reset_default_for_tests()
        assert c.get(a="x") == 0.0
        # the family is now empty, so it disappears from the render too
        assert "obs_t_reset_total" not in render_default()

    def test_histogram_helper_uses_default_registry(self) -> None:
        h1 = histogram("obs_t_hsame_seconds", "Help.", ("a",), buckets=(1.0, 5.0))
        h2 = histogram("obs_t_hsame_seconds", "Help.", ("a",), buckets=(1.0, 5.0))
        assert h1 is h2
        h1.observe(0.5, a="x")
        assert 'obs_t_hsame_seconds_count{a="x"} 1' in render_default()
