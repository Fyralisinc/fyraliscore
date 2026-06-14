"""Pure unit tests for the egress plane (M3) — redaction, planner, webhook signing.
No DB, no broker.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from lib.extensions.host_api.v1 import Capabilities, ObservationView
from services.platform.extensions.egress.planner import GrantSpec, plan_egress
from services.platform.extensions.egress import webhook
from services.platform.extensions import redaction


def _view(channel: str, content: dict) -> ObservationView:
    return ObservationView(
        id=uuid4(), tenant_id=uuid4(), occurred_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        kind="k", source_channel=channel, content=content, content_text="t",
        trust_tier="inferential_external",
    )


# --- redaction ---------------------------------------------------------------------
def test_github_redaction_keeps_login_strips_email_and_raw():
    v = _view("github:webhook", {"_raw": {"secret": 1}, "author": "octocat",
                                 "author_email": "o@e.com", "pr_title": "x"})
    out = redaction.redact(v)
    assert "author" in out.content and out.content["author"] == "octocat"  # signal kept
    assert "author_email" not in out.content                              # email stripped
    assert "_raw" not in out.content                                       # always
    assert out.content["pr_title"] == "x"


def test_default_redaction_is_strict_for_unknown_channel():
    v = _view("acme:thing", {"_raw": {}, "author": "x", "user": "y", "email": "z", "kept": 1})
    out = redaction.redact(v)
    assert out.content == {"kept": 1}  # all identity + _raw removed


# --- planner -----------------------------------------------------------------------
def test_plan_egress_filters_by_capability_and_redacts():
    v = _view("github:webhook", {"_raw": {}, "author_email": "e@x", "n": 1})
    granted = GrantSpec("ext_a", Capabilities(read_channels=("github:webhook",),
                                              substrate_read=frozenset({"observation"})))
    wrong_channel = GrantSpec("ext_b", Capabilities(read_channels=("slack:message",),
                                                    substrate_read=frozenset({"observation"})))
    no_obs = GrantSpec("ext_c", Capabilities(read_channels=("github:webhook",),
                                            substrate_read=frozenset()))
    items = plan_egress(v, [granted, wrong_channel, no_obs])
    assert [i.extension_id for i in items] == ["ext_a"]
    assert "author_email" not in items[0].view.content  # redacted in the plan


def test_plan_egress_all_channels_grant():
    v = _view("anything:x", {"k": 1})
    from lib.extensions.host_api.v1 import ALL_CHANNELS
    g = GrantSpec("ext", Capabilities(read_channels=ALL_CHANNELS,
                                     substrate_read=frozenset({"observation"})))
    assert len(plan_egress(v, [g])) == 1


# --- webhook signing ---------------------------------------------------------------
def test_webhook_sign_verify_roundtrip_and_tamper():
    body = b'{"hello":"world"}'
    sig = webhook.sign(body, "whsec_abc")
    assert sig.startswith("sha256=")
    assert webhook.verify_signature(body, sig, "whsec_abc") is True
    assert webhook.verify_signature(body, sig, "wrong") is False
    assert webhook.verify_signature(b'{"hello":"tampered"}', sig, "whsec_abc") is False
    assert webhook.verify_signature(body, "", "whsec_abc") is False
