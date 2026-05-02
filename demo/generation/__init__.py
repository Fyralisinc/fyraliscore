"""demo/generation — one-shot LLM generation of demo company snapshots.

See README.md for orchestration, cost expectations, and CLI usage. The
synthetic fallback in `services/demo/snapshot.py` continues to work
when no SQL snapshot is on disk; this package produces the richer
LLM-generated snapshot files those snapshot loaders prefer.
"""
