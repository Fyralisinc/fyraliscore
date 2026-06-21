# Ask Fyralis Latency And Cost Follow-Up

Status: parked for later.

Core finding:
- Ask Fyralis should not put deep SAGE retrieval on the default interactive path.
- Current retrieval stress reports show deep retrieval around 18s p50 and 20-22s p95 at roughly 12k models, which is too slow for "ask and get an answer right away."
- The cheap `services/product/ask` path is cost-practical because it composes answers deterministically, but latency still depends on SAGE reader retrieval.
- The older `services/product/query` path can become costlier because it forwards retrieval context into an LLM rendering call.

Recommended architecture to revisit:
- L0 cached/precomputed answers: query chips, hot company regions, CEO view summaries, target <100ms.
- L1 fast focused retrieval: bounded summaries, active models, recent evidence, target 500ms-1s.
- L2 compact answer synthesis: LLM sees a small answer packet, target 1k-3k input tokens and <2s.
- L3 deep inquiry: broad SAGE traversal, counterevidence, recurrence, topology, async/progressive, allowed 10s+.

Open questions for next pass:
- Which Ask surfaces should always stay L0/L1?
- What is the minimum answer packet for L2 without losing evidence provenance?
- How do we cache hot company regions and invalidate them after Think writes?
- What latency SLOs should product enforce for direct, quick, deep, and background Ask modes?
- How much of current SAGE reader latency is DB query, candidate expansion, graph scoring, evidence projection, and access filtering?
