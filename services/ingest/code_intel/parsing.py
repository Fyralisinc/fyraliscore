"""services/ingest/code_intel/parsing.py — language-pluggable code parsing.

A `LanguageIndexer` turns one source file into a `FileParse` (symbols +
imports + references). The registry dispatches by file extension, so adding a
language is mechanical: implement the Protocol and call `register_indexer()`.

The shipped backbone is `PythonAstIndexer` — it uses Python's stdlib `ast`
(zero external deps, precise for Python). The same Protocol is where a
tree-sitter or SCIP indexer slots in later; only the `precision` field on
reference edges changes ("heuristic" -> "exact").
"""
from __future__ import annotations

import ast
import hashlib
import posixpath
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


# ---------------------------------------------------------------------
# Parse result dataclasses (language-agnostic).
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class ParsedSymbol:
    kind: str                 # function|class|method|interface|type|const|module
    name: str
    qualified_name: str       # "module.Class.method"
    parent_qname: str | None
    start_line: int
    end_line: int
    signature: str
    docstring: str | None

    def symbol_hash(self) -> str:
        h = hashlib.sha1()
        h.update(
            f"{self.kind}|{self.qualified_name}|{self.signature}|"
            f"{self.start_line}-{self.end_line}".encode("utf-8")
        )
        return h.hexdigest()


@dataclass(frozen=True)
class ParsedImport:
    raw: str
    module_specifier: str     # resolved dotted module (relative imports resolved)
    imported_names: tuple[str, ...]


@dataclass(frozen=True)
class ParsedReference:
    from_qname: str           # enclosing symbol that makes the reference
    to_name: str              # called/used simple name
    to_qname: str | None = None   # None until resolved; SCIP fills exact
    precision: str = "heuristic"  # 'exact' | 'heuristic'


@dataclass(frozen=True)
class FileParse:
    path: str
    language: str
    module_qname: str
    symbols: tuple[ParsedSymbol, ...] = ()
    imports: tuple[ParsedImport, ...] = ()
    references: tuple[ParsedReference, ...] = ()
    parse_error: bool = False


@runtime_checkable
class LanguageIndexer(Protocol):
    language_id: str
    file_extensions: tuple[str, ...]

    def module_qname(self, rel_path: str) -> str: ...

    def parse(self, *, rel_path: str, source: bytes) -> FileParse: ...


# ---------------------------------------------------------------------
# Python (stdlib ast) indexer — the precise, zero-dependency backbone.
# ---------------------------------------------------------------------
def _rel_to_module(rel_path: str) -> str:
    """services/foo/bar.py -> services.foo.bar ; pkg/__init__.py -> pkg."""
    p = rel_path.replace("\\", "/")
    if p.endswith(".py"):
        p = p[:-3]
    if p.endswith("/__init__"):
        p = p[: -len("/__init__")]
    return p.strip("/").replace("/", ".")


class PythonAstIndexer:
    language_id = "python"
    file_extensions = (".py", ".pyi")

    def module_qname(self, rel_path: str) -> str:
        return _rel_to_module(rel_path)

    def parse(self, *, rel_path: str, source: bytes) -> FileParse:
        module = self.module_qname(rel_path)
        try:
            text = source.decode("utf-8", errors="replace")
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            # Unparseable file: record module symbol only, flag the error.
            return FileParse(
                path=rel_path,
                language=self.language_id,
                module_qname=module,
                symbols=(
                    ParsedSymbol(
                        kind="module", name=module.rsplit(".", 1)[-1] or module,
                        qualified_name=module, parent_qname=None,
                        start_line=1, end_line=1, signature=module, docstring=None,
                    ),
                ),
                parse_error=True,
            )

        symbols: list[ParsedSymbol] = [
            ParsedSymbol(
                kind="module",
                name=module.rsplit(".", 1)[-1] or module,
                qualified_name=module,
                parent_qname=None,
                start_line=1,
                end_line=max((getattr(n, "lineno", 1) for n in ast.walk(tree)), default=1),
                signature=module,
                docstring=ast.get_docstring(tree),
            )
        ]
        imports: list[ParsedImport] = []
        references: list[ParsedReference] = []

        def _sig(node: ast.AST) -> str:
            try:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    prefix = "async def " if isinstance(node, ast.AsyncFunctionDef) else "def "
                    return f"{prefix}{node.name}({ast.unparse(node.args)})"
                if isinstance(node, ast.ClassDef):
                    bases = ", ".join(ast.unparse(b) for b in node.bases)
                    return f"class {node.name}({bases})" if bases else f"class {node.name}"
            except Exception:  # noqa: BLE001 — ast.unparse can choke on exotic nodes
                pass
            return getattr(node, "name", module)

        def _collect_refs(body: list[ast.stmt], from_qname: str) -> None:
            for sub in body:
                for node in ast.walk(sub):
                    if isinstance(node, ast.Call):
                        fn = node.func
                        name = None
                        if isinstance(fn, ast.Name):
                            name = fn.id
                        elif isinstance(fn, ast.Attribute):
                            name = fn.attr
                        if name:
                            references.append(
                                ParsedReference(from_qname=from_qname, to_name=name)
                            )

        def _walk(node: ast.AST, parent_qname: str) -> None:
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    qn = f"{parent_qname}.{child.name}"
                    is_method = parent_qname != module
                    symbols.append(
                        ParsedSymbol(
                            kind="method" if is_method else "function",
                            name=child.name,
                            qualified_name=qn,
                            parent_qname=parent_qname,
                            start_line=child.lineno,
                            end_line=getattr(child, "end_lineno", child.lineno) or child.lineno,
                            signature=_sig(child),
                            docstring=ast.get_docstring(child),
                        )
                    )
                    _collect_refs(child.body, qn)
                    _walk(child, qn)  # nested defs/classes
                elif isinstance(child, ast.ClassDef):
                    qn = f"{parent_qname}.{child.name}"
                    symbols.append(
                        ParsedSymbol(
                            kind="class",
                            name=child.name,
                            qualified_name=qn,
                            parent_qname=parent_qname,
                            start_line=child.lineno,
                            end_line=getattr(child, "end_lineno", child.lineno) or child.lineno,
                            signature=_sig(child),
                            docstring=ast.get_docstring(child),
                        )
                    )
                    _walk(child, qn)

        # Imports (module-level + nested).
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(
                        ParsedImport(
                            raw=f"import {alias.name}",
                            module_specifier=alias.name,
                            imported_names=(alias.asname or alias.name.split(".")[0],),
                        )
                    )
            elif isinstance(node, ast.ImportFrom):
                spec = self._resolve_from(module, node)
                names = tuple(a.name for a in node.names)
                imports.append(
                    ParsedImport(
                        raw=f"from {'.' * (node.level or 0)}{node.module or ''} import ...",
                        module_specifier=spec,
                        imported_names=names,
                    )
                )

        _walk(tree, module)
        return FileParse(
            path=rel_path,
            language=self.language_id,
            module_qname=module,
            symbols=tuple(symbols),
            imports=tuple(imports),
            references=tuple(references),
        )

    @staticmethod
    def _resolve_from(module: str, node: ast.ImportFrom) -> str:
        """Resolve a `from . import x` / `from ..pkg import y` to a dotted module."""
        level = node.level or 0
        if level == 0:
            return node.module or ""
        # module is the importer's dotted path; go up `level` package steps.
        parts = module.split(".")
        # For a module a.b.c, level 1 = package a.b ; level 2 = a ; etc.
        base = parts[: len(parts) - level] if len(parts) >= level else []
        if node.module:
            base = base + node.module.split(".")
        return ".".join(base)


# ---------------------------------------------------------------------
# Registry — extension -> indexer instance.
# ---------------------------------------------------------------------
_REGISTRY: dict[str, LanguageIndexer] = {}


def register_indexer(indexer: LanguageIndexer) -> None:
    for ext in indexer.file_extensions:
        _REGISTRY[ext] = indexer


def get_indexer_for(rel_path: str) -> LanguageIndexer | None:
    _, ext = posixpath.splitext(rel_path.replace("\\", "/"))
    return _REGISTRY.get(ext.lower())


def supported_extensions() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


# Register the shipped backbone.
register_indexer(PythonAstIndexer())
