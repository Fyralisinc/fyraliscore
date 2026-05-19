"""services/synthetic/cutover_load.py — M-Load synthetic webhook traffic.

Signs and posts to /webhooks/{slack,github}/* at configurable QPS.
Used by the cutover dry-run (`tests/load/test_cutover_dryrun.py`) to
validate that:
  - End-to-end throughput from webhook → ingestion.raw matches
    expectations.
  - The circuit breaker correctly sees per-tenant lag.
  - Duplicate-payload dedup at the writer holds at scale.

Settled decision (M-Load): Slack + GitHub only — the two M5 cutover
providers. Gmail uses DWD push (no webhook), Discord uses Gateway
(no webhook).

Tenant pool with Zipf-ish distribution: 80% of traffic from 20% of
tenants (long tail elsewhere). Configurable run duration.

Usage:
    python -m services.synthetic.cutover_load \
        --target-url http://localhost:8080 \
        --slack-signing-secret <secret> \
        --github-webhook-secret <secret> \
        --qps 100 \
        --duration-s 3600 \
        --tenant-count 500
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Config + tenant pool.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class LoadConfig:
    target_url: str
    slack_signing_secret: str
    github_webhook_secret: str
    qps: int
    duration_s: int
    tenant_count: int
    duplicate_rate: float = 0.05  # 5% duplicate payloads
    providers: tuple[str, ...] = ("slack", "github")


def _build_tenant_pool(n: int) -> list[UUID]:
    """Deterministic pool of UUIDs for reproducible Zipf weighting."""
    rng = random.Random(0xC07E0E)
    return [UUID(int=rng.getrandbits(128)) for _ in range(n)]


def _zipf_pick(rng: random.Random, pool: list[UUID]) -> UUID:
    """Pick a tenant with Zipf-ish bias (lower-index tenants get more weight)."""
    # Simple: 80% of picks from top 20% of pool.
    n = len(pool)
    top_n = max(1, n // 5)
    if rng.random() < 0.8:
        return pool[rng.randrange(top_n)]
    return pool[rng.randrange(n)]


# ---------------------------------------------------------------------
# Payload generators + signature helpers.
# ---------------------------------------------------------------------
def _slack_payload(tenant: UUID, idempotency_seed: str) -> dict[str, Any]:
    return {
        "type": "event_callback",
        "team_id": f"T-{tenant.hex[:8]}",
        "event_id": f"Ev-{idempotency_seed}",
        "event_time": int(time.time()),
        "event": {
            "type": "message",
            "channel": f"C-{tenant.hex[:8]}",
            "user": f"U-{tenant.hex[:6]}",
            "text": f"synthetic-load {idempotency_seed}",
            "ts": f"{time.time():.6f}",
        },
    }


def _github_payload(tenant: UUID, idempotency_seed: str) -> dict[str, Any]:
    return {
        "action": "opened",
        "issue": {
            "number": int(idempotency_seed, 16) % 100000,
            "title": f"synthetic {idempotency_seed}",
            "user": {"login": f"user-{tenant.hex[:6]}"},
        },
        "installation": {"id": int(tenant.int % 1_000_000)},
        "_synthetic_seed": idempotency_seed,
    }


def _slack_sign(secret: str, ts: str, body: bytes) -> str:
    sig_base = f"v0:{ts}:".encode() + body
    digest = hmac.new(
        secret.encode(), sig_base, hashlib.sha256,
    ).hexdigest()
    return f"v0={digest}"


def _github_sign(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


# ---------------------------------------------------------------------
# Sender loop.
# ---------------------------------------------------------------------
async def run(config: LoadConfig) -> dict[str, Any]:
    """Run the synthetic load. Returns metrics dict at end."""
    rng = random.Random(0xC07E0E)
    pool = _build_tenant_pool(config.tenant_count)

    sent_total = 0
    sent_by_provider: dict[str, int] = {p: 0 for p in config.providers}
    duplicates_sent = 0
    errors: dict[str, int] = {}
    last_seeds_by_provider: dict[str, list[str]] = {
        p: [] for p in config.providers
    }
    deadline = time.monotonic() + config.duration_s
    target_interval = 1.0 / max(1, config.qps)

    async with httpx.AsyncClient(timeout=10.0) as client:
        while time.monotonic() < deadline:
            tenant = _zipf_pick(rng, pool)
            provider = rng.choice(config.providers)

            # Duplicate payload? Re-use a recent seed for this provider.
            is_dup = (
                rng.random() < config.duplicate_rate
                and last_seeds_by_provider[provider]
            )
            if is_dup:
                seed = rng.choice(last_seeds_by_provider[provider])
                duplicates_sent += 1
            else:
                seed = uuid4().hex
                last_seeds_by_provider[provider].append(seed)
                # Keep buffer small.
                if len(last_seeds_by_provider[provider]) > 100:
                    last_seeds_by_provider[provider].pop(0)

            try:
                if provider == "slack":
                    payload = _slack_payload(tenant, seed)
                    body = json.dumps(payload).encode("utf-8")
                    ts = str(int(time.time()))
                    sig = _slack_sign(config.slack_signing_secret, ts, body)
                    r = await client.post(
                        f"{config.target_url}/webhooks/slack/events",
                        content=body,
                        headers={
                            "Content-Type": "application/json",
                            "X-Slack-Request-Timestamp": ts,
                            "X-Slack-Signature": sig,
                        },
                    )
                else:  # github
                    payload = _github_payload(tenant, seed)
                    body = json.dumps(payload).encode("utf-8")
                    sig = _github_sign(config.github_webhook_secret, body)
                    r = await client.post(
                        f"{config.target_url}/webhooks/github/events",
                        content=body,
                        headers={
                            "Content-Type": "application/json",
                            "X-Hub-Signature-256": sig,
                            "X-GitHub-Event": "issues",
                            "X-GitHub-Delivery": seed,
                        },
                    )
                if r.status_code >= 400:
                    bucket = f"{provider}_{r.status_code}"
                    errors[bucket] = errors.get(bucket, 0) + 1
                else:
                    sent_total += 1
                    sent_by_provider[provider] += 1
            except httpx.HTTPError as exc:
                bucket = f"{provider}_transport_error"
                errors[bucket] = errors.get(bucket, 0) + 1
                log.warning("synthetic.transport_error",
                            extra={"provider": provider,
                                   "error": str(exc)[:200]})

            await asyncio.sleep(target_interval)

    return {
        "sent_total": sent_total,
        "sent_by_provider": sent_by_provider,
        "duplicates_sent": duplicates_sent,
        "errors": errors,
        "qps_actual": sent_total / max(1, config.duration_s),
        "duration_s": config.duration_s,
        "tenant_count": config.tenant_count,
    }


def _parse_args() -> LoadConfig:
    p = argparse.ArgumentParser()
    p.add_argument("--target-url", required=True)
    p.add_argument(
        "--slack-signing-secret",
        default=os.environ.get("SLACK_SIGNING_SECRET", "test-secret"),
    )
    p.add_argument(
        "--github-webhook-secret",
        default=os.environ.get("GITHUB_WEBHOOK_SECRET", "test-secret"),
    )
    p.add_argument("--qps", type=int, default=100)
    p.add_argument("--duration-s", type=int, default=3600)
    p.add_argument("--tenant-count", type=int, default=500)
    args = p.parse_args()
    return LoadConfig(
        target_url=args.target_url,
        slack_signing_secret=args.slack_signing_secret,
        github_webhook_secret=args.github_webhook_secret,
        qps=args.qps,
        duration_s=args.duration_s,
        tenant_count=args.tenant_count,
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("SYNTHETIC_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = _parse_args()
    metrics = asyncio.run(run(config))
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()


__all__ = ["LoadConfig", "main", "run"]
