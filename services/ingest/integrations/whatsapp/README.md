# WhatsApp ingestion — Phase 1 (LIVE)

Real-time ingestion of WhatsApp Business Platform (Cloud API) traffic: inbound
customer **messages** and outbound **delivery statuses**, delivered by Meta as
signed HTTPS webhooks and written straight into `observations` via the normal
inline `ingest()` path (dedup + Think trigger included).

Backfill (Coexistence history webhook / BSP bulk export / chat-export import) is
a **deferred later phase** — see the research notes; it is intentionally not
wired here.

## What's in the box

| Piece | Path |
|---|---|
| Webhook router (GET verify + signed POST fan-out + viewer + register) | [`services/app/gateway/whatsapp_router.py`](../../../app/gateway/whatsapp_router.py) |
| Handlers (`whatsapp:message`, `whatsapp:status`) | [`services/ingest/ingestion/handlers/whatsapp.py`](../../ingestion/handlers/whatsapp.py) |
| Signature (X-Hub-Signature-256) | [`signature.py`](signature.py) |
| Dedup keys | `idempotency.whatsapp_message` / `whatsapp_status` |
| Creds/routing table | migration `db/migrations/0144_whatsapp.sql` → `whatsapp_installations` |
| Self-contained runner | [`scripts/whatsapp_live_server.py`](../../../../scripts/whatsapp_live_server.py) |
| Local simulator | [`scripts/whatsapp_simulate.py`](../../../../scripts/whatsapp_simulate.py) |

## Option A — see it locally in 60s (no Meta account needed)

```bash
# 0) apply the migration (once)
docker exec -i company_os_postgres psql -U company_os -d company_os \
  < db/migrations/0144_whatsapp.sql

# 1) run the self-contained server (Postgres only — no Kafka/S3/Ollama)
export DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os
export WHATSAPP_VERIFY_TOKEN=demo-verify-token
.venv/bin/python scripts/whatsapp_live_server.py            # serves :8000

# 2) open the live viewer, paste the tenant id:
#    http://localhost:8000/debug/whatsapp?tenant_id=00000000-0000-0000-0000-000000000001

# 3) register an install + fire messages (another terminal)
TENANT=00000000-0000-0000-0000-000000000001
.venv/bin/python scripts/whatsapp_simulate.py register \
  --tenant-id $TENANT --phone-number-id 109988887776 --app-secret demosecret --verify-token demo-verify-token
.venv/bin/python scripts/whatsapp_simulate.py send \
  --phone-number-id 109988887776 --app-secret demosecret \
  --from 14155550123 --name "Alice Buyer" --text "Do you have this in size M?"
.venv/bin/python scripts/whatsapp_simulate.py status \
  --phone-number-id 109988887776 --app-secret demosecret --to 14155550123 --state read
```

Messages appear in the viewer within ~2s.

## Option B — real WhatsApp traffic via Meta

1. Run the server (or the full gateway), then expose it:
   `ngrok http 8000` → copy the `https://…ngrok…` URL.
2. In Meta **App dashboard → WhatsApp → Configuration → Webhook**:
   - **Callback URL**: `https://<ngrok>/integrations/whatsapp/webhook`
   - **Verify token**: the value of `WHATSAPP_VERIFY_TOKEN`
   - Subscribe to the **`messages`** field (covers inbound messages + statuses).
3. Register the installation with your real creds (so the receiver can verify
   the HMAC and route to your tenant):
   ```bash
   python scripts/whatsapp_simulate.py register \
     --tenant-id <your-tenant-uuid> \
     --phone-number-id <Meta phone_number_id> \
     --app-secret <Meta App Secret> \
     --verify-token <WHATSAPP_VERIFY_TOKEN>
   ```
4. Send a WhatsApp message to your business number → it lands in the viewer.

### Creds you provide (from Meta App dashboard / WhatsApp setup)

| Cred | Where it comes from | Used for |
|---|---|---|
| `phone_number_id` | WhatsApp → API Setup | webhook → tenant routing key |
| **App Secret** | App → Settings → Basic | verifying `X-Hub-Signature-256` |
| **Verify token** | you choose it; set in Meta webhook config | the GET subscribe handshake |
| `access_token` *(optional, Phase 1)* | System User token | only needed later for media download / backfill |

## Notes / production hardening (deferred)

- **Secrets are dev-grade plaintext** in `whatsapp_installations`. Production
  should move `app_secret`/`access_token` behind the envelope-encrypted secret
  store (a `secret_ref`, like `provider_installations`).
- **PII**: WhatsApp content is high-PII (customer phone/name/order data) and is
  received in plaintext. The egress redaction layer is a known gap (see research).
- **Media**: messages carry a media *id*; downloading the bytes (2-step Graph GET,
  5-min URL expiry) is not wired in Phase 1.
- **Kafka data-plane**: Phase 1 uses the inline `ingest()` path. To put WhatsApp
  on the Kafka data plane, add `"whatsapp"` to `SourceLiteral` + the 5 source
  lists + provision topics (the standard add-a-source checklist).
- `WHATSAPP_ALLOW_UNSIGNED=1` bypasses signature verification — **local only**.
