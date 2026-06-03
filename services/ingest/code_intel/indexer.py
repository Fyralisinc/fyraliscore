"""services/ingest/code_intel/indexer.py — build a code-graph snapshot from a working copy.

`index_working_copy` walks a local checkout, parses each supported file, resolves
imports (module -> file) and references (name -> symbol, heuristic), and writes a
complete `code_snapshots` row + files/symbols/edges + pending embeddings.

For the production fetch path this working copy comes from a shallow `git clone`
with the installation token (Part A1 of the plan); for dogfooding/tests it is any
local directory. The graph is identical regardless of how the bytes arrived.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction

from services.ingest.code_intel.graph import CodeGraphRepo
from services.ingest.code_intel.parsing import FileParse, get_indexer_for, supported_extensions

_DEFAULT_IGNORE_DIRS = frozenset({
    ".git", "node_modules", "vendor", "dist", "build", "__pycache__",
    ".venv", "venv", ".mypy_cache", ".pytest_cache", ".tox", ".idea", ".next",
})
_GENERATED_MARKERS = (b"@generated", b"DO NOT EDIT")


@dataclass
class IndexStats:
    snapshot_id: UUID
    commit_sha: str
    files: int
    symbols: int
    edges: int
    parse_errors: int
    embeddings_queued: int


async def index_working_copy(
    *,
    pool: Any,
    tenant_id: UUID,
    repo_full_name: str,
    root_path: str,
    commit_sha: str,
    branch: str = "main",
    index_kind: str = "full",
    parent_snapshot_id: UUID | None = None,
    max_files: int = 5000,
    queue_embeddings: bool = True,
) -> IndexStats:
    parsed = _scan_and_parse(root_path, max_files=max_files)

    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        repo = CodeGraphRepo(ctx)
        snapshot_id = await repo.create_snapshot(
            tenant_id=tenant_id, repo_full_name=repo_full_name, branch=branch,
            commit_sha=commit_sha, index_kind=index_kind,
            parent_snapshot_id=parent_snapshot_id,
        )
        # A re-index of an already-ready sha is a no-op (idempotent).
        snap = await repo.get_snapshot(snapshot_id)
        if snap is not None and snap.status == "ready":
            return IndexStats(snapshot_id, commit_sha, snap.file_count,
                              snap.symbol_count, snap.edge_count, 0, 0)

        try:
            stats = await _write_graph(
                repo, tenant_id=tenant_id, snapshot_id=snapshot_id,
                parsed=parsed, queue_embeddings=queue_embeddings,
            )
        except Exception as exc:  # noqa: BLE001
            await repo.mark_failed(snapshot_id, f"{type(exc).__name__}: {exc}")
            raise

        await repo.mark_ready(
            snapshot_id, files=stats["files"], symbols=stats["symbols"],
            edges=stats["edges"], parse_errors=stats["parse_errors"],
        )
        return IndexStats(
            snapshot_id, commit_sha, stats["files"], stats["symbols"],
            stats["edges"], stats["parse_errors"], stats["embeddings_queued"],
        )


@dataclass
class _ParsedFile:
    rel_path: str
    blob_sha: str
    size_bytes: int
    line_count: int
    language: str | None
    parse: FileParse | None  # None for binary/unsupported


def _scan_and_parse(root_path: str, *, max_files: int) -> list[_ParsedFile]:
    out: list[_ParsedFile] = []
    exts = set(supported_extensions())
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirnames[:] = [d for d in dirnames if d not in _DEFAULT_IGNORE_DIRS]
        for fn in sorted(filenames):
            if len(out) >= max_files:
                return out
            _, ext = os.path.splitext(fn)
            if ext.lower() not in exts:
                continue
            abspath = os.path.join(dirpath, fn)
            rel = os.path.relpath(abspath, root_path).replace("\\", "/")
            try:
                with open(abspath, "rb") as fh:
                    source = fh.read()
            except OSError:
                continue
            blob_sha = hashlib.sha1(source).hexdigest()
            size = len(source)
            lines = source.count(b"\n") + 1
            is_generated = any(m in source[:4096] for m in _GENERATED_MARKERS)
            indexer = get_indexer_for(rel)
            parse = None if is_generated or indexer is None else indexer.parse(
                rel_path=rel, source=source
            )
            out.append(_ParsedFile(
                rel_path=rel, blob_sha=blob_sha, size_bytes=size, line_count=lines,
                language=(indexer.language_id if indexer else None),
                parse=parse,
            ))
    return out


async def _write_graph(
    repo: CodeGraphRepo, *, tenant_id: UUID, snapshot_id: UUID,
    parsed: list[_ParsedFile], queue_embeddings: bool,
) -> dict[str, int]:
    # 1. files + module->file_id map
    path_to_fid: dict[str, UUID] = {}
    module_to_fid: dict[str, UUID] = {}
    parse_errors = 0
    for pf in parsed:
        fid = await repo.insert_file(
            tenant_id=tenant_id, snapshot_id=snapshot_id, path=pf.rel_path,
            language=pf.language, blob_sha=pf.blob_sha, size_bytes=pf.size_bytes,
            line_count=pf.line_count, is_generated=(pf.language is None),
        )
        path_to_fid[pf.rel_path] = fid
        if pf.parse is not None:
            module_to_fid[pf.parse.module_qname] = fid
            if pf.parse.parse_error:
                parse_errors += 1

    # 2. symbols — assign ids, build qname->id and name->[ids]
    qname_to_sid: dict[str, UUID] = {}
    name_to_sids: dict[str, list[UUID]] = {}
    sym_rows: list[tuple] = []
    sym_file: dict[UUID, UUID] = {}
    pending_symbol_meta: list[tuple[UUID, str, str, str, str | None]] = []  # sid, file, qname, sig, doc

    for pf in parsed:
        if pf.parse is None:
            continue
        fid = path_to_fid[pf.rel_path]
        for sym in pf.parse.symbols:
            sid = uuid7()
            qname_to_sid[sym.qualified_name] = sid
            name_to_sids.setdefault(sym.name, []).append(sid)
            sym_file[sid] = fid
            pending_symbol_meta.append(
                (sid, fid, sym.qualified_name, sym.signature, sym.docstring)
            )

    # second pass: emit rows with resolved parent_symbol_id
    for pf in parsed:
        if pf.parse is None:
            continue
        fid = path_to_fid[pf.rel_path]
        for sym in pf.parse.symbols:
            sid = qname_to_sid[sym.qualified_name]
            parent_sid = qname_to_sid.get(sym.parent_qname) if sym.parent_qname else None
            sym_rows.append((
                sid, tenant_id, snapshot_id, fid, sym.kind, sym.name,
                sym.qualified_name, parent_sid, sym.start_line, sym.end_line,
                sym.signature, sym.docstring, sym.symbol_hash(),
            ))
    await repo.insert_symbols(sym_rows)

    # 3. edges
    edge_rows: list[tuple] = []

    def _edge(kind, src_sym=None, src_file=None, dst_sym=None, dst_file=None,
              unresolved=None, precision="heuristic"):
        edge_rows.append((
            uuid7(), tenant_id, snapshot_id, kind, src_sym, src_file,
            dst_sym, dst_file, unresolved, precision,
        ))

    for pf in parsed:
        if pf.parse is None:
            continue
        src_fid = path_to_fid[pf.rel_path]
        # contains: parent symbol -> child symbol
        for sym in pf.parse.symbols:
            if sym.parent_qname:
                parent_sid = qname_to_sid.get(sym.parent_qname)
                child_sid = qname_to_sid.get(sym.qualified_name)
                if parent_sid and child_sid:
                    _edge("contains", src_sym=parent_sid, dst_sym=child_sid, precision="exact")
        # imports: file -> module (resolved to dst_file, else unresolved)
        for imp in pf.parse.imports:
            spec = imp.module_specifier
            dst_fid = _resolve_module(spec, module_to_fid)
            if dst_fid is not None and dst_fid != src_fid:
                _edge("imports", src_file=src_fid, dst_file=dst_fid, precision="exact")
            elif dst_fid is None and spec:
                _edge("imports", src_file=src_fid, unresolved=spec, precision="heuristic")
        # references: symbol -> symbol (heuristic, unique-name resolution)
        for ref in pf.parse.references:
            src_sid = qname_to_sid.get(ref.from_qname)
            if not src_sid:
                continue
            candidates = name_to_sids.get(ref.to_name, [])
            # resolve only when unambiguous and not self
            targets = [s for s in candidates if s != src_sid]
            if len(targets) == 1:
                _edge("references", src_sym=src_sid, dst_sym=targets[0], precision="heuristic")

    await repo.insert_edges(edge_rows)

    # 4. embeddings (pending — filled best-effort later)
    emb_rows: list[tuple] = []
    if queue_embeddings:
        for sid, fid, qname, sig, doc in pending_symbol_meta:
            text = f"{qname}\n{sig}\n{doc or ''}".strip()[:1900]
            emb_rows.append((uuid7(), tenant_id, snapshot_id, sid, fid, 0, text))
        await repo.insert_embeddings_pending(emb_rows)

    return {
        "files": len(parsed),
        "symbols": len(sym_rows),
        "edges": len(edge_rows),
        "parse_errors": parse_errors,
        "embeddings_queued": len(emb_rows),
    }


def _resolve_module(spec: str, module_to_fid: dict[str, UUID]) -> UUID | None:
    """Resolve a dotted module spec to a file id, trying progressively shorter
    prefixes (so `services.foo.bar.baz` resolves to module `services.foo.bar`
    when `baz` is a symbol, or to `services.foo` package, etc.)."""
    if not spec:
        return None
    if spec in module_to_fid:
        return module_to_fid[spec]
    parts = spec.split(".")
    for i in range(len(parts) - 1, 0, -1):
        cand = ".".join(parts[:i])
        if cand in module_to_fid:
            return module_to_fid[cand]
    return None
