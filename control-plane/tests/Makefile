# Makefile for the Fyralis BYOC control-plane end-to-end smoke + integration suite.
#
#   make smoke        # run the scripted no-docker end-to-end smoke (exit 0 = pass)
#   make test         # run the pytest wrapper (granular per-step pass/fail)
#   make live         # run the smoke and ALSO attempt the docker-only path
#   make smoke-keep   # run the smoke and keep the temp sandbox for inspection
#   make all          # smoke + test
#
# PYTHON resolution: prefer an explicit $(PYTHON), else the fyraliscore venv that
# already has cryptography/fastapi/httpx/h11 installed, else a plain `python3`.

HERE := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

VENV_PY := /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python
PYTHON ?= $(shell if [ -x "$(VENV_PY)" ]; then echo "$(VENV_PY)"; else command -v python3 || command -v python; fi)

.PHONY: smoke test live smoke-keep all help

help:
	@echo "Targets:"
	@echo "  make smoke       - scripted no-docker end-to-end smoke (e2e_smoke.py)"
	@echo "  make test        - pytest wrapper (test_e2e.py), per-step assertions"
	@echo "  make live        - smoke + attempt the docker-only path (--live)"
	@echo "  make smoke-keep  - smoke, keeping the temp sandbox for inspection"
	@echo "  make all         - smoke then test"
	@echo "Using PYTHON=$(PYTHON)"

smoke:
	cd "$(HERE)" && "$(PYTHON)" e2e_smoke.py

test:
	cd "$(HERE)" && "$(PYTHON)" -m pytest test_e2e.py -v -p no:cacheprovider

live:
	cd "$(HERE)" && "$(PYTHON)" e2e_smoke.py --live

smoke-keep:
	cd "$(HERE)" && "$(PYTHON)" e2e_smoke.py --keep

all: smoke test
