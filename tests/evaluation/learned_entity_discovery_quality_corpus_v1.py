"""Frozen independent corpus for the learned entity-discovery quality run.

This file was authored without reading the repository's existing entity corpora or
holdouts.  The hash covers source text, batch/channel metadata, and exact-span/type
gold.  It must not be edited after the first provider call.
"""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid5

NAMESPACE = UUID("9aee7e73-e644-4a69-9ce5-0d222da60531")

# mention tuples are (literal surface, coarse type). Repeated surfaces are forbidden
# in a signal so offsets are unambiguous and become part of FROZEN_CORPUS.
_ROWS = (
    # Batch 1 — Slack, international names, nested/quoted text, and negatives.
    ("slack", 'Mina: “Please ask Nguyễn Thị Lan to review Project Aurora.”', (("Nguyễn Thị Lan", "person"), ("Project Aurora", "project"))),
    ("slack", "@channel the build is green; thanks everyone", ()),
    ("slack", "Søren Kierkegaard owns the Northstar migration.", (("Søren Kierkegaard", "person"), ("Northstar migration", "workstream"))),
    ("slack", "The customer asked the manager to call tomorrow.", ()),
    ("slack", "Replying to ‘Acme GmbH chose Helios’: legal still needs the DPA.", (("Acme GmbH", "customer"), ("Helios", "product"))),
    ("slack", "FYI: 2026-07-17 09:30 UTC — no action required.", ()),
    ("slack", "李小龙 paired with the Data Reliability team.", (("李小龙", "person"), ("Data Reliability", "team"))),
    ("slack", "Can someone check it?", ()),
    ("slack", "The `AtlasClient.connect()` call fails only in staging.", (("AtlasClient", "system"),)),
    ("slack", "✅ shipped; 🚫 no rollback; 0 incidents", ()),

    # Batch 2 — Jira-like records, identifiers and metadata distractors.
    ("jira", "OPS-1842: Priya Nair will replace the Kafka Bridge timeout.", (("OPS-1842", "commitment"), ("Priya Nair", "person"), ("Kafka Bridge", "system"))),
    ("jira", "Reporter: admin | Priority: P2 | Status: Open", ()),
    ("jira", "客户成功平台 rollout blocked by Contoso Japan.", (("客户成功平台", "product"), ("Contoso Japan", "customer"))),
    ("jira", "TODO investigate flaky test; assignee TBD", ()),
    ("jira", "Decision DEC-77: retire Mercury API after Q4.", (("DEC-77", "decision"), ("Mercury API", "system"))),
    ("jira", "Labels: backend, needs-triage, july", ()),
    ("jira", "Fatima الزهراء assigned to Initiative Cedar.", (("Fatima الزهراء", "person"), ("Initiative Cedar", "project"))),
    ("jira", "Error 404 at /v1/items/{id}; request_id=ab12", ()),
    ("jira", "TEAM-9 tracks the Platform Security team charter.", (("TEAM-9", "goal"), ("Platform Security", "team"))),
    ("jira", "The director approved the proposal.", ()),

    # Batch 3 — email, signatures/quoted replies/boundaryless strings.
    ("email", "Subject: Renewal\nNordlys AS accepted the Fjord Analytics quote.", (("Nordlys AS", "customer"), ("Fjord Analytics", "product"))),
    ("email", "Hi all, following up on yesterday's discussion. Regards, Team", ()),
    ("email", "Aiko Tanaka <aiko@example.test> leads Project Komorebi.", (("Aiko Tanaka", "person"), ("Project Komorebi", "project"))),
    ("email", "CONFIDENTIALITY NOTICE: intended recipient only.", ()),
    ("email", "> Omar said: \"Bluebird Squad will support Banco Sol.\"", (("Bluebird Squad", "team"), ("Banco Sol", "customer"))),
    ("email", "Sent from my phone", ()),
    ("email", "Please provision svc-payments-prod for the Kintsugi launch.", (("svc-payments-prod", "system"), ("Kintsugi launch", "workstream"))),
    ("email", "To: undisclosed-recipients; Cc: archive", ()),
    ("email", "María-José Carreño confirmed Commitment C-204.", (("María-José Carreño", "person"), ("Commitment C-204", "commitment"))),
    ("email", "-----Original Message-----\nYes, that works for me.", ()),

    # Batch 4 — code, handles, punctuation boundaries, role ambiguity.
    ("slack", "Deploy `Orchid::Scheduler` after Amélie Dubois signs off.", (("Orchid::Scheduler", "system"), ("Amélie Dubois", "person"))),
    ("slack", "if(user_id==null){return false;} // ordinary guard", ()),
    ("slack", "@joao says CaféOps—not Café Ops—is the owning team.", (("CaféOps", "team"),)),
    ("slack", "The VP and two engineers are in the room.", ()),
    ("slack", "Use [Nimbus Console](https://invalid.test/nimbus) for TEN-443.", (("Nimbus Console", "product"), ("TEN-443", "resource"))),
    ("slack", "{" + '"level":"info","ok":true,"count":7' + "}", ()),
    ("slack", "Олександр Коваль moved Goal G-19 into discovery.", (("Олександр Коваль", "person"), ("Goal G-19", "goal"))),
    ("slack", "maybe the client thing belongs to them?", ()),
    ("slack", "EdgeCaseLabs/VertexSync failed at the slash boundary.", (("EdgeCaseLabs", "customer"), ("VertexSync", "system"))),
    ("slack", "... typing ...", ()),

    # Batch 5 — Jira mixed prose and adversarial near-entities.
    ("jira", "RISK-31 links Elena Petrova to the Horizon decision.", (("RISK-31", "resource"), ("Elena Petrova", "person"), ("Horizon decision", "decision"))),
    ("jira", "Component: unknown | Environment: none | Fix: n/a", ()),
    ("jira", "São Paulo Growth team supports Mercado Estrela.", (("São Paulo Growth", "team"), ("Mercado Estrela", "customer"))),
    ("jira", "User reports that the product is slow.", ()),
    ("jira", "PRJ_0042 delivers the Saffron Ledger prototype.", (("PRJ_0042", "project"), ("Saffron Ledger", "product"))),
    ("jira", "Stack trace: NullPointerException at line 42", ()),
    ("jira", "Nkiru Okafor owns Workstream Delta-Blue.", (("Nkiru Okafor", "person"), ("Workstream Delta-Blue", "workstream"))),
    ("jira", "Acceptance criteria: fast, secure, usable", ()),
    ("jira", "RES-88 grants access to the Snowcap Dataset.", (("RES-88", "resource"), ("Snowcap Dataset", "resource"))),
    ("jira", "Someone from finance should confirm.", ()),

    # Batch 6 — email nested quotes, Unicode, IDs, and clean negatives.
    ("email", "Subject: Escalation\nDr. Iñaki López contacted Zürcher Werke AG.", (("Dr. Iñaki López", "person"), ("Zürcher Werke AG", "customer"))),
    ("email", "Automatic reply: I am away until Monday.", ()),
    ("email", "> > Leila wrote: ‘Team Qamar chose Project Sandglass.’", (("Team Qamar", "team"), ("Project Sandglass", "project"))),
    ("email", "Message-ID: <20260717.1234@example.test>\nMIME-Version: 1.0", ()),
    ("email", "Kenji佐藤 approved DECISION-Ω7 for the Redwood Engine.", (("Kenji佐藤", "person"), ("DECISION-Ω7", "decision"), ("Redwood Engine", "system"))),
    ("email", "Please see the attachment; there is no attachment.", ()),
    ("email", "The Makani Programme commits to deliver CMT-909.", (("Makani Programme", "project"), ("CMT-909", "commitment"))),
    ("email", "unsubscribe | preferences | privacy policy", ()),
    ("email", "Anaïs Nin will demo LumenDesk to Étoile Santé.", (("Anaïs Nin", "person"), ("LumenDesk", "product"), ("Étoile Santé", "customer"))),
    ("email", "Re: Re: Re: quick question\nNever mind.", ()),
)


def _freeze() -> tuple[dict, ...]:
    frozen = []
    for index, (source, text, mentions) in enumerate(_ROWS, 1):
        signal_id = str(uuid5(NAMESPACE, f"signal-{index:02d}"))
        gold = []
        for mention_index, (surface, entity_type) in enumerate(mentions, 1):
            assert text.count(surface) == 1, (index, surface)
            start = text.index(surface)
            gold.append({
                "mention_id": f"m-{index:02d}-{mention_index}",
                "start": start,
                "end": start + len(surface),
                "surface": surface,
                "entity_type": entity_type,
            })
        frozen.append({
            "signal_id": signal_id,
            "batch_id": f"batch-{((index - 1) // 10) + 1}",
            "source_type": source,
            "slack_context": "threaded" if source == "slack" else "not_slack",
            "text": text,
            "gold": gold,
        })
    return tuple(frozen)


FROZEN_CORPUS = _freeze()
FROZEN_SHA256 = "36de9f1dd6424c86817ead36f96e8fca2ee72cf3262846776d7613ad4563700f"


def canonical_bytes() -> bytes:
    return json.dumps(
        FROZEN_CORPUS, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def computed_sha256() -> str:
    return hashlib.sha256(canonical_bytes()).hexdigest()
