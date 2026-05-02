"""Hand-authored demo bundles. Sit alongside the LLM generation
pipeline in ../generate.py — same output shape (GeneratedBundle), same
validate.py and sql_emit.py path; just authored deterministically by
people instead of by an LLM, so re-running produces an identical SQL
snapshot without spending API budget.

Run via: python -m demo.generation.built.<company>
"""
