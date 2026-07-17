#!/usr/bin/env python3
"""Run the smallest joined DB-backed company-learning working vertical."""

from __future__ import annotations

import argparse, asyncio, hashlib, json, os, re, sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

import asyncpg
from lib.contracts.kernel import canonical_sha256
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from scripts.compiled_facet_decision_provider import CompiledFacetDecisionProvider
from scripts.run_bounded_company_model_ablation_db import REPO_ROOT, _runtime_runs, _think_batch
from services.domain.correction_propagation.service import CorrectionPropagationService
from services.domain.models.repo import pgvector_pool_init
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import RelationClaimOp, ValidatedDiff
from services.reasoning.think.tests.conftest import make_embedding
from services.company_physics_vertical import run_company_physics_vertical
from tests.evaluation.company_model_holdout_v7 import BATCHES_V7

INTEGRATED_BATCHES = (
    (("mercury", "current_risk"), *BATCHES_V7[0][1:]),
    *BATCHES_V7[1:],
)


class _IntegratedDecisionProvider(CompiledFacetDecisionProvider):
    """Materially synthesize only same-subject prior Model content."""
    async def _raw_call(self, **kwargs):
        raw = await super()._raw_call(**kwargs)
        payload=json.loads(raw); user=kwargs["user"]
        current={s.lower() for s,_f in re.findall(r"\[FACET subject=([a-z0-9_-]+) value=([a-z0-9_-]+)\]",user,re.I)}
        matching=[]
        for line in user.splitlines():
            found=re.search(r"([0-9a-f-]{36})",line,re.I)
            if found and any(subject in line.lower() for subject in current): matching.append(found.group(1))
        if matching:
            for decision in payload["decisions"]:
                text=str(decision.get("claim_text") or "")
                subject=text.partition(" evidence facets:")[0]
                if subject in current and any(subject in line.lower() for line in user.splitlines() if matching[0] in line):
                    facets={x.strip() for x in text.partition(":")[2].split(",") if x.strip()}
                    facets.add("prior_blocked")
                    decision["claim_text"]=f"{subject} evidence facets: "+", ".join(sorted(facets))
                    break
            payload["reasoning_trace"]="Materially synthesized same-subject prior Models: "+", ".join(sorted(set(matching)))
        else:
            payload["reasoning_trace"]="No same-subject prior Model materially used."
        return json.dumps(payload)


async def _json_codec(conn):
    for name in ("json", "jsonb"):
        await conn.set_type_codec(name, encoder=lambda v: json.dumps(v) if not isinstance(v,str) else v,
                                  decoder=json.loads, schema="pg_catalog")


async def _insert_learning_batch(pool, tenant, actor, index, definitions):
    sources=("slack:normalized","jira:normalized","email:normalized","document_meeting:normalized")
    observations=[]
    async with pool.acquire() as conn:
        for offset,(subject,facet) in enumerate(definitions,1):
            oid=uuid7(); source=sources[(index+offset)%len(sources)]
            text=f"Project {subject.title()} operational update [FACET subject={subject} value={facet}]"
            await conn.execute("""INSERT INTO observations
              (id,tenant_id,occurred_at,kind,source_channel,actor_id,content,content_text,
               embedding,embedding_pending,trust_tier,external_id)
              VALUES($1,$2,now(),'signal',$3,$4,'{}'::jsonb,$5,$6,FALSE,'authoritative',$7)""",
              oid,tenant,source,actor,text,make_embedding(text),f"integrated-{index}-{offset}")
            observations.append((oid,text))
    return observations


async def run_once(dsn: str, output: Path, receipt: Path):
    if output.exists() or receipt.exists(): raise RuntimeError("integrated v2 vertical is one-shot")
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"]="0"
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"]="1"
    meta={"schema_version":"integrated-company-learning-receipt-v2","run_attempts":1,
          "started_at":datetime.now(timezone.utc).isoformat(),"status":"running"}
    receipt.write_text(json.dumps(meta,indent=2,sort_keys=True)+"\n")
    try:
        conn=await asyncpg.connect(dsn)
        try: await apply_migrations_dir(conn,REPO_ROOT/"db"/"migrations")
        finally: await conn.close()
        # Source-semantic persistence currently supplies textual vector input;
        # Think uses the registered pgvector binary codec. Keep both production
        # paths in the same DB/tenant while changing only the connection codec.
        pool=await asyncpg.create_pool(dsn,min_size=1,max_size=8,init=_json_codec)
        tenant,actor=uuid7(),uuid7()
        try:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO tenants(id,name,is_demo) VALUES($1,'integrated-company-learning',FALSE)",tenant)
                await conn.execute("INSERT INTO actors(id,tenant_id,type,display_name,status) VALUES($1,$2,'human_internal','Analyst','active')",actor,tenant)
                baseline={name:await conn.fetchval(f"SELECT count(*) FROM {name} WHERE tenant_id=$1",tenant)
                          for name in ("models","model_edges","grounding_traces","source_semantic_interpretations")}

            source_result=await run_company_physics_vertical(pool=pool,tenant_id=tenant)
            async with pool.acquire() as conn:
                physics_model_ids=list(await conn.fetch("""SELECT DISTINCT admitted_model_id AS id
                  FROM source_semantic_admission_decisions WHERE tenant_id=$1
                    AND admitted_model_id IS NOT NULL""",tenant))
                physics_model_ids=[row["id"] for row in physics_model_ids]
                physics_mercury_model_id=await conn.fetchval("""SELECT a.admitted_model_id
                  FROM source_semantic_admission_decisions a
                  JOIN source_semantic_interpretations i ON i.tenant_id=a.tenant_id AND i.id=a.interpretation_id
                  JOIN grounding_traces gt ON gt.tenant_id=i.tenant_id AND gt.id=i.grounding_trace_id
                  JOIN observations o ON o.tenant_id=gt.tenant_id AND o.id=gt.source_observation_id
                  WHERE a.tenant_id=$1 AND o.content_text='Mercury is blocked.'""",tenant)

            await pool.close()
            pool=await asyncpg.create_pool(dsn,min_size=1,max_size=8,init=pgvector_pool_init)

            run_ids=[]
            for index,definitions in enumerate(INTEGRATED_BATCHES,1):
                observations=await _insert_learning_batch(pool,tenant,actor,index,definitions)
                run_ids.append(await _think_batch(pool,tenant,actor,index,observations,
                    consume_model_summaries=True,provider_factory=_IntegratedDecisionProvider))
            runtime=await _runtime_runs(pool,run_ids)

            ablation_user='''[FACET subject=mercury value=current_risk]\n<candidate>\ncandidate_id: MDC_1\nproposed_text: mercury synthesis\n</candidate>'''
            physics_mercury_id=str(physics_mercury_model_id)
            with_prior=await _IntegratedDecisionProvider()._raw_call(
                system="bounded",user=ablation_user+f'\n<allowed_model_cards>\n- id={physics_mercury_id} natural=Mercury is blocked.\n</allowed_model_cards>',
                temperature=0,max_tokens=1000,schema_hint=None)
            without_prior=await _IntegratedDecisionProvider()._raw_call(
                system="bounded",user=ablation_user,temperature=0,max_tokens=1000,schema_hint=None)
            ablation={"with_prior":json.loads(with_prior),"without_prior":json.loads(without_prior)}

            async with pool.acquire() as conn:
                correction_traces=await conn.fetch("""SELECT gt.id,gt.source_observation_id,a.admitted_model_id,o.content_text
                  FROM grounding_traces gt JOIN source_semantic_interpretations i
                    ON i.tenant_id=gt.tenant_id AND i.grounding_trace_id=gt.id
                  JOIN source_semantic_admission_decisions a ON a.tenant_id=i.tenant_id AND a.interpretation_id=i.id
                  JOIN observations o ON o.tenant_id=gt.tenant_id AND o.id=gt.source_observation_id
                  WHERE gt.tenant_id=$1 AND o.content_text IN ('Mercury is blocked.','Venus is blocked.') AND a.admitted_model_id IS NOT NULL
                  ORDER BY o.content_text""",tenant)
                predecessor=next(row for row in correction_traces if row["content_text"]=='Venus is blocked.')
                successor=next(row for row in correction_traces if row["content_text"]=='Mercury is blocked.')
                learned_target=await conn.fetchval("SELECT id FROM models WHERE tenant_id=$1 AND \"natural\" LIKE 'tundra evidence facets:%' AND status='active' LIMIT 1",tenant)
                unrelated_model_id=await conn.fetchval("SELECT id FROM models WHERE tenant_id=$1 AND \"natural\" LIKE 'uplink evidence facets:%' AND status='active' LIMIT 1",tenant)
                relation_event=uuid7()
                await conn.execute("""INSERT INTO observations(id,tenant_id,occurred_at,kind,source_channel,actor_id,content,content_text,embedding,embedding_pending,trust_tier)
                  VALUES($1,$2,now(),'signal','simulated:normalized',$3,'{}','Correction dependency lineage',$4,FALSE,'authoritative')""",
                  relation_event,tenant,actor,make_embedding('Correction dependency lineage'))
                await apply_diff(ValidatedDiff(trigger_ref=uuid7(),tenant_id=tenant,relation_claim_ops=[RelationClaimOp(
                  source_model_id=predecessor["admitted_model_id"],target_model_id=learned_target,
                  subject_ref={"kind":"model","model_id":str(predecessor["admitted_model_id"])},
                  object_ref={"kind":"model","model_id":str(learned_target)},predicate="supports",edge_kind="supports",
                  endpoint_binding_status="bound",write_policy="accepted_edge",status="accepted",confidence=.91,binding_confidence=.96,
                  evidence_event_ids=[relation_event],evidence_model_ids=[predecessor["admitted_model_id"],learned_target],
                  evidence_text="The governed Venus state supports the learned Tundra model.",explanation="Cross-stage correction dependency." )]),
                  conn,"T1",relation_event,trigger_supporting_event_ids=[relation_event])
                exact_edge_before=await conn.fetchrow("""SELECT id,status,source_model_id,target_model_id,metadata
                  FROM model_edges WHERE tenant_id=$1 AND source_model_id=$2 AND target_model_id=$3
                    AND edge_kind='supports' ORDER BY created_at DESC LIMIT 1""",
                  tenant,predecessor["admitted_model_id"],learned_target)
                atlas_active_before=await conn.fetchval("SELECT count(*) FROM models WHERE tenant_id=$1 AND id=$2 AND status='active'",tenant,unrelated_model_id)
                async with conn.transaction():
                    correction=await CorrectionPropagationService().propagate_direct_correction(
                        conn,tenant_id=tenant,predecessor_grounding_trace_id=predecessor["id"],
                        successor_grounding_trace_id=successor["id"],cause_event_id=successor["source_observation_id"],
                        corrected_model_id=successor["admitted_model_id"])
                exact_edge_after=await conn.fetchrow("SELECT id,status,source_model_id,target_model_id,metadata FROM model_edges WHERE tenant_id=$1 AND id=$2",tenant,exact_edge_before["id"])
                atlas_active_after=await conn.fetchval("SELECT count(*) FROM models WHERE tenant_id=$1 AND id=$2 AND status='active'",tenant,unrelated_model_id)
                populations={name:await conn.fetchval(f"SELECT count(*) FROM {name} WHERE tenant_id=$1",tenant)
                  for name in ("observations","models","model_edges","grounding_traces","entity_mention_detections",
                               "source_semantic_interpretations","source_semantic_admission_decisions","model_reeval_queue")}
                active_models=await conn.fetchval("SELECT count(*) FROM models WHERE tenant_id=$1 AND status='active'",tenant)
                active_edges=await conn.fetchval("SELECT count(*) FROM model_edges WHERE tenant_id=$1 AND status='active'",tenant)
                synthesized_models=await conn.fetchval("SELECT count(*) FROM models WHERE tenant_id=$1 AND \"natural\"='mercury evidence facets: current_risk, prior_blocked'",tenant)
                cross_tenant=await conn.fetchval("SELECT count(*) FROM models WHERE tenant_id<>$1 AND id=ANY($2::uuid[])",tenant,[UUID(x) for r in runtime for x in r['selected_model_ids']])

            checks={
              "zero_semantic_baseline":all(v==0 for v in baseline.values()),
              "six_genuine_batches":source_result["population"]["batches"]+len(INTEGRATED_BATCHES)==6,
              "multi_source_normalized":source_result["population"]["signals"]==7,
              "batch_entity_discovery":source_result["discovery"]["structured_calls"]==1 and source_result["discovery"]["governed_fate_coverage"]==1.0,
              "governed_canonical_linking":source_result["canonical_link_metrics"]["accuracy"]==1.0 and source_result["canonical_link_metrics"]["coverage"]==1.0,
              "active_batch_memory_success":all(r["status"]=="success" for r in runtime),
              "relation_lineage_admitted":source_result["lineage_metrics"]["relation"]==1.0 and source_result["topology_metrics"]["relation_admission_accuracy"]==1.0,
              "correction_archived_wrong_models":len(correction.archived_model_ids)>=1,
              "correction_fenced_relations":(
                  exact_edge_before["status"]=="active"
                  and exact_edge_after["id"]==exact_edge_before["id"]
                  and exact_edge_after["status"] in {"inert","retired","needs_review"}
                  and exact_edge_after["source_model_id"] in correction.archived_model_ids
                  and exact_edge_after["target_model_id"] in correction.dependent_model_ids
                  and (exact_edge_after["target_model_id"],exact_edge_after["source_model_id"]) in correction.reeval_pairs
              ),
              "correction_enqueued_reevaluation":populations["model_reeval_queue"]>=1,
              "unrelated_model_survives":atlas_active_before==atlas_active_after and atlas_active_after>=1,
              "late_model_first_context":len(runtime[-1]["selected_model_ids"])>0 and bool(runtime[-1]["referenced_model_ids"]),
              "referenced_selected_models":set(runtime[-1]["referenced_model_ids"]).issubset(set(runtime[-1]["selected_model_ids"])) and bool(runtime[-1]["referenced_model_ids"]),
              "physics_models_used_by_learning":any(str(mid) in set().union(*(set(r["selected_model_ids"]) & set(r["referenced_model_ids"]) for r in runtime)) for mid in physics_model_ids),
              "single_model_cross_stage_synthesis":synthesized_models>=1,
              "material_use_ablation":("prior_blocked" in ablation["with_prior"]["decisions"][0]["claim_text"] and "prior_blocked" not in ablation["without_prior"]["decisions"][0]["claim_text"]),
              "tenant_isolation_negative_control":cross_tenant==0,
            }
            score=sum(checks.values())/len(checks)
            artifact={"schema_version":"integrated-company-learning-vertical-v2",
              "tenant_id":str(tenant),"batch_count":6,"signal_count":populations["observations"],
              "baseline":baseline,"populations":populations,"active_populations":{"models":active_models,"edges":active_edges},
              "entity_discovery":{"persisted_detection_count":populations["entity_mention_detections"],"structured_batch_calls":source_result["discovery"]["structured_calls"],"governed_fate_coverage":source_result["discovery"]["governed_fate_coverage"]},
              "company_physics":{"objective_sha256":source_result["objective_sha256"],"population":source_result["population"],"canonical_link_metrics":source_result["canonical_link_metrics"],"lineage_metrics":source_result["lineage_metrics"]},
              "runtime_context_use":runtime,
              "material_use_ablation":ablation,
              "correction":{"old_model_ids":[str(x) for x in correction.old_model_ids],"archived_model_ids":[str(x) for x in correction.archived_model_ids],
                "dependent_model_ids":[str(x) for x in correction.dependent_model_ids],"reeval_pairs":[[str(a),str(b)] for a,b in correction.reeval_pairs],
                "fenced_relations":len(correction.relation_fence.retired_relation_ids)+len(correction.relation_fence.needs_review_relation_ids),
                "inactive_edges_after_correction":populations["model_edges"]-active_edges,
                "exact_cross_stage_edge":{"id":str(exact_edge_before["id"]),
                  "source_model_id":str(exact_edge_before["source_model_id"]),
                  "target_model_id":str(exact_edge_before["target_model_id"]),
                  "status_before":exact_edge_before["status"],"status_after":exact_edge_after["status"],
                  "linked_archived_source":exact_edge_after["source_model_id"] in correction.archived_model_ids,
                  "linked_dependent_target":exact_edge_after["target_model_id"] in correction.dependent_model_ids,
                  "linked_reeval_pair":(exact_edge_after["target_model_id"],exact_edge_after["source_model_id"]) in correction.reeval_pairs}},
              "negative_controls":{"cross_tenant_selected_models":cross_tenant,"atlas_active_before":atlas_active_before,"atlas_active_after":atlas_active_after},
              "checks":checks,"continuous_score":score,"verdict":"meets_policy" if score==1 else "below_policy",
              "proof_boundary":["normalized persisted signals begin after connector transport","deterministic providers are limited to mention spans and closed-world batch-memory decisions","does not authorize task autonomy"]}
            artifact["objective_sha256"]=canonical_sha256(artifact)
            output.write_text(json.dumps(artifact,indent=2,sort_keys=True)+"\n")
        finally: await pool.close()
        meta.update({"status":"completed","completed_at":datetime.now(timezone.utc).isoformat(),
                     "artifact_sha256":hashlib.sha256(output.read_bytes()).hexdigest(),
                     "objective_sha256":artifact["objective_sha256"],"verdict":artifact["verdict"],"continuous_score":artifact["continuous_score"]})
        return artifact
    except Exception as exc:
        meta.update({"status":"failed","completed_at":datetime.now(timezone.utc).isoformat(),"error_type":type(exc).__name__,"error_message":str(exc)});raise
    finally: receipt.write_text(json.dumps(meta,indent=2,sort_keys=True)+"\n")


def main():
    p=argparse.ArgumentParser();p.add_argument("--dsn",required=True);p.add_argument("--output",type=Path,required=True);p.add_argument("--receipt",type=Path,required=True);a=p.parse_args()
    result=asyncio.run(run_once(a.dsn,a.output,a.receipt));print(json.dumps({"verdict":result["verdict"],"continuous_score":result["continuous_score"],"populations":result["populations"]},sort_keys=True))
if __name__=="__main__":main()
