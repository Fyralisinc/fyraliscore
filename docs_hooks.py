"""MkDocs build hook — make the ported legacy doc trees build under ``--strict``.

``docs/ingestion/`` and ``docs/github-intelligence/`` were written as
GitHub-relative docs: they link straight to source files
(``../../services/foo.py``) and a couple of specs that don't live under
``docs/``. MkDocs ``--strict`` rejects any link that escapes ``docs_dir`` or
points at a missing file, so this hook rewrites those links **at build time
only** — the markdown on disk is left untouched (so it still reads correctly when
viewed directly on GitHub).

Rules (applied only to pages under the two legacy trees):

* A link that escapes ``docs/`` (i.e. a source file) → an absolute GitHub *blob*
  URL at the correct repo-relative path, preserving any ``#Lxx`` line anchor.
* A link to a known-missing target → de-linked (the label text is kept).
* A valid intra-site link (resolves to another built page) → left untouched.
* Links inside fenced code blocks are never rewritten.

Registered via ``hooks:`` in ``mkdocs.yml``. No third-party plugin required.
"""

from __future__ import annotations

import posixpath
import re
from pathlib import Path

# Trees this hook is allowed to touch (relative to docs_dir, posix).
# `reference/` holds the centralized codebase docs (CODEBASE-ARCHITECTURE.md,
# CODEBASE-MANAGEMENT.md, FYRALIS.md) which link straight to source files.
_LEGACY_PREFIXES = (
    "ingestion/",
    "github-intelligence/",
    "reference/",
    "memory-layer.md",  # standalone memory/model-layer reference; links to source
)

# Targets that don't exist anywhere in the repo → drop the link, keep the words.
_MISSING_TARGETS = {"03-low-level-design.md", "05-lld-amendments.md"}

# GitHub blob base for source-file links. Points at the integration trunk.
_BLOB_BASE = "https://github.com/Fyralisinc/fyraliscore/blob/main/"

# [text](target) — target captured greedily up to the closing paren; a link
# "title" (rare) is split off in the replacer.
_LINK_RE = re.compile(r"(?<!\!)\[([^\]]+)\]\(([^)]+)\)")
_FENCE_RE = re.compile(r"^\s*(```|~~~)")
_SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.-]*://")


def on_page_markdown(markdown, *, page, config, files, **kwargs):  # noqa: D401
    src = page.file.src_uri  # e.g. "ingestion/sources/jira.md"
    if not src.startswith(_LEGACY_PREFIXES):
        return markdown

    docs_dir = Path(config["docs_dir"]).resolve()
    repo_root = docs_dir.parent
    page_dir = posixpath.dirname(src)

    def _rewrite_target(text: str, target: str) -> str:
        raw = target.strip()
        # Preserve an optional link title:  (url "title")
        bits = raw.split(None, 1)
        url, title = bits[0], (bits[1] if len(bits) > 1 else "")
        suffix = f" {title}" if title else ""

        # Leave absolute URLs, mail, and pure-anchor links alone.
        if _SCHEME_RE.match(url) or url.startswith(("#", "mailto:")):
            return f"[{text}]({raw})"

        path, _, frag = url.partition("#")
        if not path:
            return f"[{text}]({raw})"

        if posixpath.basename(path) in _MISSING_TARGETS:
            return text  # de-link a dangling reference

        joined = posixpath.normpath(posixpath.join(page_dir, path))
        # Valid link that stays inside docs/ and resolves to a real file → keep.
        if not joined.startswith("..") and (docs_dir / joined).exists():
            return f"[{text}]({raw})"

        # Otherwise it escapes docs/ — map it to a repo-relative blob URL.
        abs_target = Path(posixpath.normpath((docs_dir / page_dir / path).as_posix()))
        try:
            rel = abs_target.resolve().relative_to(repo_root).as_posix()
        except ValueError:
            return text  # outside the repo entirely → drop the link
        blob = _BLOB_BASE + rel + (f"#{frag}" if frag else "")
        return f"[{text}]({blob}{suffix})"

    out: list[str] = []
    in_fence = False
    for line in markdown.splitlines(keepends=True):
        if _FENCE_RE.match(line):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        out.append(_LINK_RE.sub(lambda m: _rewrite_target(m.group(1), m.group(2)), line))
    return "".join(out)
