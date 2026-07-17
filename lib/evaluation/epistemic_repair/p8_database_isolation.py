"""Literal fresh-database lifecycle for isolated P8 scale cells."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_population import ScaleCell
from lib.evaluation.epistemic_repair.p8_scale_runner import (
    ActualScaleCell,
    WarmPairDiagnostic,
    run_scale_cell,
    run_warm_pair_diagnostic,
)
from lib.shared.migrations import apply_migrations_dir


_SAFE_DATABASE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


@dataclass(frozen=True, slots=True)
class DatabaseIdentity:
    database_name: str
    database_oid: int
    server_address: str | None
    server_port: int | None
    schema_migration_count: int
    schema_digest: str
    identity_digest: str


@dataclass(frozen=True, slots=True)
class DatabaseIsolationProof:
    template_identity: DatabaseIdentity
    cell_identity: DatabaseIdentity
    identities_distinct: bool
    cell: ActualScaleCell
    template_dropped: bool
    cell_database_dropped: bool
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class ExistingTemplateIsolationProof:
    template_identity: DatabaseIdentity
    template_active_sessions_before: int
    cells: tuple[DatabaseIsolationProof, ...]
    all_database_oids_distinct: bool
    all_cell_databases_dropped: bool
    template_preserved: bool
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class IsolatedWarmPairProof:
    template_identity: DatabaseIdentity
    clone_identity: DatabaseIdentity
    template_active_sessions_before: int
    clone_external_backends_before: int
    diagnostic: WarmPairDiagnostic
    clone_database_dropped: bool
    evidence_digest: str


def _dsn_for_database(dsn: str, database: str) -> str:
    if not _SAFE_DATABASE.fullmatch(database):
        raise ValueError("unsafe generated database name")
    parsed = urlsplit(dsn)
    return urlunsplit((parsed.scheme, parsed.netloc, f"/{database}", parsed.query, parsed.fragment))


async def _identity(dsn: str, *, migration_count: int) -> DatabaseIdentity:
    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            """SELECT current_database() AS name,
                      (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS oid,
                      inet_server_addr()::text AS address, inet_server_port() AS port"""
        )
        schema_rows = await conn.fetch(
            """SELECT table_name, column_name, data_type, is_nullable
               FROM information_schema.columns WHERE table_schema='public'
               ORDER BY table_name,column_name"""
        )
    finally:
        await conn.close()
    schema_digest = canonical_sha256([dict(item) for item in schema_rows])
    body = {"name": row["name"], "oid": row["oid"], "address": row["address"],
            "port": row["port"], "migrations": migration_count,
            "schema_digest": schema_digest}
    return DatabaseIdentity(
        row["name"], row["oid"], row["address"], row["port"], migration_count,
        schema_digest,
        canonical_sha256(body),
    )


async def prove_fresh_database_cell(
    admin_dsn: str, *, migrations_dir: Path, cell: ScaleCell,
) -> DatabaseIsolationProof:
    suffix = uuid4().hex[:10]
    template_name, cell_name = f"p8_template_{suffix}", f"p8_cell_{suffix}"
    template_dsn = _dsn_for_database(admin_dsn, template_name)
    cell_dsn = _dsn_for_database(admin_dsn, cell_name)
    template_dropped = cell_dropped = False
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{template_name}"')
        template = await asyncpg.connect(template_dsn)
        try:
            await apply_migrations_dir(template, migrations_dir)
        finally:
            await template.close()
        migration_count = len(tuple(migrations_dir.glob("*.sql")))
        template_identity = await _identity(template_dsn, migration_count=migration_count)
        await admin.execute(f'ALTER DATABASE "{template_name}" WITH ALLOW_CONNECTIONS false')
        await admin.execute(f'CREATE DATABASE "{cell_name}" TEMPLATE "{template_name}"')
        cell_identity = await _identity(cell_dsn, migration_count=migration_count)
        measured = await run_scale_cell(cell_dsn, cell)
        await admin.execute(f'DROP DATABASE "{cell_name}"')
        cell_dropped = True
        await admin.execute(f'DROP DATABASE "{template_name}"')
        template_dropped = True
    finally:
        for name in (cell_name, template_name):
            try:
                await admin.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
                    name,
                )
                await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
            except Exception:
                pass
        await admin.close()
    distinct = template_identity.database_oid != cell_identity.database_oid
    payload = {
        "template": asdict(template_identity), "cell": asdict(cell_identity),
        "cell_result_digest": measured.evidence_digest, "distinct": distinct,
        "dropped": [template_dropped, cell_dropped],
    }
    return DatabaseIsolationProof(
        template_identity, cell_identity, distinct, measured,
        template_dropped, cell_dropped, canonical_sha256(payload),
    )


async def prove_existing_template_cells(
    admin_dsn: str, *, template_name: str, migrations_dir: Path,
    cells: tuple[ScaleCell, ...],
) -> ExistingTemplateIsolationProof:
    if not _SAFE_DATABASE.fullmatch(template_name):
        raise ValueError("unsafe template database name")
    migration_count = len(tuple(migrations_dir.glob("*.sql")))
    template_dsn = _dsn_for_database(admin_dsn, template_name)
    admin = await asyncpg.connect(admin_dsn)
    active = await admin.fetchval(
        "SELECT count(*)::int FROM pg_stat_activity WHERE datname=$1",
        template_name,
    )
    if active:
        await admin.close()
        raise RuntimeError(f"template database has {active} active sessions")
    template_identity = await _identity(template_dsn, migration_count=migration_count)
    check = await asyncpg.connect(template_dsn)
    try:
        required = await check.fetchval(
            """SELECT count(*)::int FROM information_schema.tables
               WHERE table_schema='public' AND table_name=ANY($1::text[])""",
            ["model_truth_versions", "company_learning_barriers", "company_learning_context_decisions"],
        )
    finally:
        await check.close()
    if required != 3:
        await admin.close()
        raise RuntimeError("template is not migrated through the P8 truth/barrier schema")
    proofs: list[DatabaseIsolationProof] = []
    try:
        for ordinal, cell in enumerate(cells, 1):
            cell_name = f"p8_cell_{uuid4().hex[:10]}"
            cell_dsn = _dsn_for_database(admin_dsn, cell_name)
            dropped = False
            try:
                await admin.execute(f'CREATE DATABASE "{cell_name}" TEMPLATE "{template_name}"')
                identity = await _identity(cell_dsn, migration_count=migration_count)
                measured = await run_scale_cell(cell_dsn, cell)
                await admin.execute(f'DROP DATABASE "{cell_name}"')
                dropped = True
            finally:
                if not dropped:
                    await admin.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
                        cell_name,
                    )
                    await admin.execute(f'DROP DATABASE IF EXISTS "{cell_name}"')
            distinct = identity.database_oid != template_identity.database_oid
            payload = {"ordinal": ordinal, "template": asdict(template_identity),
                       "cell": asdict(identity), "result": measured.evidence_digest,
                       "dropped": dropped}
            proofs.append(DatabaseIsolationProof(
                template_identity, identity, distinct, measured,
                False, dropped, canonical_sha256(payload),
            ))
    finally:
        await admin.close()
    oids = [template_identity.database_oid, *(proof.cell_identity.database_oid for proof in proofs)]
    all_distinct = len(set(oids)) == len(oids)
    payload = {"template": asdict(template_identity), "active": active,
               "proofs": [asdict(proof) for proof in proofs]}
    return ExistingTemplateIsolationProof(
        template_identity, active, tuple(proofs), all_distinct,
        all(proof.cell_database_dropped for proof in proofs), True,
        canonical_sha256(payload),
    )


async def run_isolated_warm_pair(
    admin_dsn: str, *, template_name: str, migrations_dir: Path,
    batch_size: int = 10, memory_horizon_batches: int = 50,
    repetitions: int = 5, pool_size: int = 20,
) -> IsolatedWarmPairProof:
    if not _SAFE_DATABASE.fullmatch(template_name):
        raise ValueError("unsafe template database name")
    migration_count = len(tuple(migrations_dir.glob("*.sql")))
    clone_name = f"p8_warm_{uuid4().hex[:10]}"
    template_dsn = _dsn_for_database(admin_dsn, template_name)
    clone_dsn = _dsn_for_database(admin_dsn, clone_name)
    admin = await asyncpg.connect(admin_dsn)
    dropped = False
    try:
        active = await admin.fetchval(
            "SELECT count(*)::int FROM pg_stat_activity WHERE datname=$1",
            template_name,
        )
        if active:
            raise RuntimeError(f"template database has {active} active sessions")
        template_identity = await _identity(template_dsn, migration_count=migration_count)
        await admin.execute(f'CREATE DATABASE "{clone_name}" TEMPLATE "{template_name}"')
        clone_identity = await _identity(clone_dsn, migration_count=migration_count)
        baseline = await asyncpg.connect(clone_dsn)
        try:
            external = await baseline.fetchval(
                """SELECT count(*)::int FROM pg_stat_activity
                   WHERE datname=current_database() AND pid<>pg_backend_pid()"""
            )
        finally:
            await baseline.close()
        if external:
            raise RuntimeError(f"disposable clone has {external} unexpected backends")
        diagnostic = await run_warm_pair_diagnostic(
            clone_dsn, batch_size=batch_size,
            memory_horizon_batches=memory_horizon_batches,
            repetitions=repetitions, pool_size=pool_size,
        )
        await admin.execute(f'DROP DATABASE "{clone_name}"')
        dropped = True
    finally:
        if not dropped:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
                clone_name,
            )
            await admin.execute(f'DROP DATABASE IF EXISTS "{clone_name}"')
        await admin.close()
    payload = {
        "template": asdict(template_identity), "clone": asdict(clone_identity),
        "template_active": active, "clone_external": external,
        "diagnostic": asdict(diagnostic), "clone_dropped": dropped,
    }
    return IsolatedWarmPairProof(
        template_identity, clone_identity, active, external, diagnostic,
        dropped, canonical_sha256(payload),
    )
