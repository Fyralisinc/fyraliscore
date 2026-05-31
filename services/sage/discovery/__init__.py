"""services.sage.discovery — Phase 10 discovery shortcuts + negative memory.

This sub-package lives in the **Discovery Utility Layer**
(fyralis-sage-synthesis-self-evolution.md §2). Nothing in here is
canonical truth — both `discovery_shortcuts` and `negative_memory`
record learned retrieval utility / learned dead-ends. The repos here
intentionally surface that distinction: utility scores are mutable,
failures decay rather than fail-loudly, and every negative memory
carries an `expires_at` so today's noise can become tomorrow's
useful signal when company reality shifts.

Backing schema: db/migrations/0052_sage_discovery_and_negative_memory.sql.
"""
