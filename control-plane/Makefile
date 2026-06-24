# =============================================================================
# control-plane/Makefile — operator shortcuts for the Fyralis BYOC control plane
# -----------------------------------------------------------------------------
#   make bootstrap            first-run: CA + signing + demo onboard + stack up
#   make up                   bring the stack up (build + -d)
#   make down                 stop the stack (keep volumes)
#   make onboard TENANT=foo   onboard another tenant (REGION=, PLAN= optional)
#   make smoke                run the end-to-end smoke (tests/e2e_smoke.py)
#   make logs                 tail the stack
#   make clean                down -v + remove generated CA/keys/runtime material
#   make config               `docker compose config` (parse check)
# =============================================================================

COMPOSE      ?= docker compose
COMPOSE_FILE ?= docker-compose.control-plane.yml
DATAPLANE_NET ?= dataplane-net

# Prefer the repo dev venv, else python3.
PYTHON := $(shell [ -x /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python ] \
            && echo /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
            || echo python3)

# onboard args (overridable: make onboard TENANT=globex REGION=eu-west PLAN=standard)
TENANT ?= acme
REGION ?= us-east-1
PLAN   ?= standard

.PHONY: bootstrap up down onboard smoke logs clean config help

help:
	@sed -n '3,13p' $(MAKEFILE_LIST) | sed 's/^# \{0,1\}//'

## first-run one-command entry
bootstrap:
	./bootstrap.sh

## ensure the external data-plane network exists, then bring the stack up
up:
	@docker network inspect $(DATAPLANE_NET) >/dev/null 2>&1 || docker network create $(DATAPLANE_NET) >/dev/null
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --build
	@echo "Console:  http://localhost:8080"
	@echo "Grafana:  http://localhost:3000"
	@echo "(internal stores — Mimir/Loki/CP-Prometheus — are cp-net-only, no host port; reach via Grafana/auth-proxy)"

down:
	$(COMPOSE) -f $(COMPOSE_FILE) down

## onboard a tenant: make onboard TENANT=globex REGION=eu-west
onboard:
	$(PYTHON) onboarding/onboard.py --tenant $(TENANT) --region $(REGION) --plan $(PLAN) --embedded-console

## end-to-end smoke (no docker required; mocks Mimir, runs the real CP code paths)
smoke:
	$(PYTHON) tests/e2e_smoke.py

logs:
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=120

## parse-check the master compose
config:
	$(COMPOSE) -f $(COMPOSE_FILE) config >/dev/null && echo "compose config OK"

## tear down + remove ALL generated trust/runtime material (full reset)
clean:
	-$(COMPOSE) -f $(COMPOSE_FILE) down -v
	rm -rf _runtime .env onboarding/bundles auth-proxy/tls ca/pki signing/keys signing/trust_root.json
	printf '{}\n' > ca/tenant_registry.json
	@echo "cleaned generated CA/keys/runtime material (ca/tenant_registry.json reset to {})"
