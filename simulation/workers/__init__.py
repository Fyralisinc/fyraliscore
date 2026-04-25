"""Simulation channel workers — CLI scripts that emit SyntheticSignals
via services/synthetic/core.inject() to fake non-Slack channels
(GitHub PR, GitHub issues, email, calendar, Linear).

See simulation/workers/_common.py for the shared bootstrap that every
worker goes through (env guard, DB pool, actor seeding, run_id).
"""
