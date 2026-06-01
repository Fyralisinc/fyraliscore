"""CLI entry for `python -m services.ingestion.writers`.

Per M2 work-order §M2.4.
"""
from __future__ import annotations

from services.ingestion.writers.observation_writer import main


if __name__ == "__main__":
    main()
