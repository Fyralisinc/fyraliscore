#!/usr/bin/env python3
"""Safe, repeatable Codex home cleanup.

The script always inspects first and backs up important Codex files before any
cleanup action. If Codex is currently running, it stays in inspect-only mode.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
import tomllib
from collections.abc import Iterable
from pathlib import Path
from typing import Any


CODEX_PROCESS_MARKERS = (
    "/applications/codex.app/",
    "codex app-server",
    "/.codex/computer-use/",
    "/codex computer use.app/",
    "/cua_node/bin/node_repl",
    "openai.chatgpt-",
)
BACKUP_FILES = (
    "config.toml",
    "auth.json",
    ".codex-global-state.json",
    ".codex-global-state.json.bak",
    "session_index.jsonl",
    "installation_id",
)
BACKUP_DIRS = ("memories", "skills", "plugins", "automations")
SQLITE_BACKUP_PATTERNS = ("state_*.sqlite", "memories_*.sqlite", "goals_*.sqlite")
PIN_KEYS = {"pinned", "is_pinned", "favorite", "starred", "saved"}
WORKSPACE_KEYS = {"cwd", "workspace", "workspace_root", "workspace_roots"}


def utc_stamp() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def human_bytes(value: int | float | None) -> str:
    if value is None:
        return "unknown"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def rel(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def disk_usage_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        result = subprocess.run(
            ["du", "-sk", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
        return int(result.stdout.split()[0]) * 1024
    except Exception:
        if path.is_file():
            return path.stat().st_size
        total = 0
        for child in path.rglob("*"):
            try:
                if child.is_file() and not child.is_symlink():
                    total += child.stat().st_size
            except OSError:
                continue
        return total


def count_files(path: Path, pattern: str = "*") -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return 1
    return sum(1 for child in path.rglob(pattern) if child.is_file())


def largest_files(path: Path, limit: int = 10) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.is_file():
        return [{"path": str(path), "bytes": path.stat().st_size}]
    files: list[tuple[int, Path]] = []
    for child in path.rglob("*"):
        try:
            if child.is_file() and not child.is_symlink():
                files.append((child.stat().st_size, child))
        except OSError:
            continue
    return [
        {"path": str(child), "bytes": size}
        for size, child in sorted(files, reverse=True)[:limit]
    ]


def top_level_space(codex_dir: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not codex_dir.exists():
        return entries
    for child in sorted(codex_dir.iterdir()):
        try:
            entries.append(
                {
                    "path": str(child),
                    "bytes": disk_usage_bytes(child),
                    "kind": "dir" if child.is_dir() else "file",
                }
            )
        except OSError as exc:
            entries.append({"path": str(child), "error": str(exc)})
    return sorted(entries, key=lambda item: item.get("bytes", 0), reverse=True)


def detect_codex_processes(script_name: str) -> list[dict[str, Any]]:
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,command="],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        return [{"error": f"could not inspect process table: {exc}"}]

    current_pid = os.getpid()
    processes: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        command = parts[2]
        lowered = command.lower()
        if pid == current_pid or script_name in command:
            continue
        if any(marker in lowered for marker in CODEX_PROCESS_MARKERS):
            processes.append({"pid": pid, "ppid": ppid, "command": command})
    return processes


def copy_file(src: Path, dst: Path, warnings: list[str]) -> bool:
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst, follow_symlinks=False)
        return True
    except Exception as exc:
        warnings.append(f"backup failed for {src}: {exc}")
        return False


def copy_tree(src: Path, dst: Path, warnings: list[str]) -> bool:
    ok = True
    for root, dirs, files in os.walk(src, followlinks=False):
        root_path = Path(root)
        target_root = dst / root_path.relative_to(src)
        try:
            target_root.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            warnings.append(f"backup mkdir failed for {target_root}: {exc}")
            ok = False
            continue

        for dirname in list(dirs):
            source_child = root_path / dirname
            target_child = target_root / dirname
            if source_child.is_symlink():
                try:
                    target_child.symlink_to(os.readlink(source_child))
                except FileExistsError:
                    pass
                except Exception as exc:
                    warnings.append(f"backup symlink failed for {source_child}: {exc}")
                    ok = False
                dirs.remove(dirname)

        for filename in files:
            source_child = root_path / filename
            target_child = target_root / filename
            try:
                mode = source_child.lstat().st_mode
                if stat.S_ISSOCK(mode) or stat.S_ISFIFO(mode):
                    warnings.append(f"backup skipped special file {source_child}")
                    continue
                if source_child.is_symlink():
                    target_child.symlink_to(os.readlink(source_child))
                else:
                    shutil.copy2(source_child, target_child, follow_symlinks=False)
            except FileExistsError:
                continue
            except Exception as exc:
                warnings.append(f"backup failed for {source_child}: {exc}")
                ok = False
    return ok


def sqlite_backup(src: Path, dst: Path, warnings: list[str]) -> bool:
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        src_uri = f"file:{src}?mode=ro"
        with sqlite3.connect(src_uri, uri=True, timeout=5) as source:
            with sqlite3.connect(dst) as target:
                source.backup(target)
                target.commit()
        return True
    except Exception as exc:
        warnings.append(f"sqlite backup failed for {src}: {exc}")
        return False


def backup_important(codex_dir: Path, backup_path: Path) -> dict[str, Any]:
    warnings: list[str] = []
    copied: list[str] = []
    missing: list[str] = []
    critical_ok = True

    files_dir = backup_path / "files"
    dirs_dir = backup_path / "directories"
    db_dir = backup_path / "sqlite"

    for name in BACKUP_FILES:
        src = codex_dir / name
        if not src.exists():
            missing.append(name)
            continue
        if copy_file(src, files_dir / name, warnings):
            copied.append(name)
        else:
            critical_ok = False

    for name in BACKUP_DIRS:
        src = codex_dir / name
        if not src.exists():
            missing.append(name)
            continue
        if copy_tree(src, dirs_dir / name, warnings):
            copied.append(name)
        else:
            critical_ok = False

    db_sources: list[Path] = []
    for pattern in SQLITE_BACKUP_PATTERNS:
        db_sources.extend(sorted(codex_dir.glob(pattern)))
    for src in db_sources:
        if sqlite_backup(src, db_dir / src.name, warnings):
            copied.append(src.name)
        else:
            critical_ok = False

    manifest = {
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "codex_dir": str(codex_dir),
        "copied": copied,
        "missing": missing,
        "warnings": warnings,
        "critical_ok": critical_ok,
    }
    (backup_path / "backup_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest


def parse_json_file(path: Path) -> tuple[bool, str | None]:
    try:
        json.loads(path.read_text(encoding="utf-8"))
        return True, None
    except Exception as exc:
        return False, str(exc)


def sqlite_integrity(path: Path) -> dict[str, Any]:
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5) as conn:
            rows = conn.execute("PRAGMA integrity_check").fetchall()
        values = [row[0] for row in rows]
        return {"path": str(path), "ok": values == ["ok"], "result": values}
    except Exception as exc:
        return {"path": str(path), "ok": False, "error": str(exc)}


def truthy_pin(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "pinned", "favorite", "saved"}
    if isinstance(value, (int, float)):
        return value != 0
    return bool(value)


def collect_pinned_session_ids(codex_dir: Path) -> set[str]:
    pinned: set[str] = set()
    index = codex_dir / "session_index.jsonl"
    if index.exists():
        with index.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                session_id = record.get("id")
                if not isinstance(session_id, str):
                    continue
                if any(truthy_pin(record.get(key)) for key in PIN_KEYS):
                    pinned.add(session_id)
    return pinned


def extract_session_id(path: Path) -> str | None:
    match = re.search(
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        path.name,
        re.IGNORECASE,
    )
    return match.group(1).lower() if match else None


def unique_dest(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for number in range(1, 10_000):
        candidate = path.with_name(f"{stem}.{number}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"could not find unique destination for {path}")


def old_session_candidates(
    codex_dir: Path,
    archive_age_days: int,
    pinned_ids: set[str],
) -> list[dict[str, Any]]:
    sessions_dir = codex_dir / "sessions"
    cutoff = time.time() - archive_age_days * 86400
    candidates: list[dict[str, Any]] = []
    if not sessions_dir.exists():
        return candidates
    for path in sorted(sessions_dir.rglob("*.jsonl")):
        try:
            info = path.stat()
        except OSError:
            continue
        session_id = extract_session_id(path)
        is_pinned = bool(session_id and session_id in pinned_ids)
        if info.st_mtime <= cutoff and not is_pinned:
            candidates.append(
                {
                    "path": str(path),
                    "bytes": info.st_size,
                    "mtime": dt.datetime.fromtimestamp(
                        info.st_mtime, dt.UTC
                    ).isoformat(),
                    "session_id": session_id,
                }
            )
    return candidates


def archive_old_sessions(
    codex_dir: Path,
    candidates: list[dict[str, Any]],
    stamp: str,
    dry_run: bool,
) -> list[dict[str, Any]]:
    sessions_dir = codex_dir / "sessions"
    archive_root = codex_dir / "archived_sessions" / stamp
    moved: list[dict[str, Any]] = []
    for candidate in candidates:
        src = Path(candidate["path"])
        try:
            relative = src.relative_to(sessions_dir)
        except ValueError:
            relative = Path(src.name)
        dst = unique_dest(archive_root / relative)
        if not dry_run:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
        moved.append({**candidate, "archived_to": str(dst)})
    return moved


def find_worktree_candidates(codex_dir: Path, archive_age_days: int) -> list[dict[str, Any]]:
    cutoff = time.time() - archive_age_days * 86400
    roots = [
        codex_dir / "worktrees",
        codex_dir / "tmp" / "worktrees",
        codex_dir / ".tmp" / "worktrees",
    ]
    candidates: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for child in root.iterdir():
            if not child.is_dir() or child in seen:
                continue
            seen.add(child)
            git_marker = child / ".git"
            if not git_marker.exists():
                continue
            try:
                info = child.stat()
            except OSError:
                continue
            if info.st_mtime <= cutoff:
                candidates.append(
                    {
                        "path": str(child),
                        "bytes": disk_usage_bytes(child),
                        "mtime": dt.datetime.fromtimestamp(
                            info.st_mtime, dt.UTC
                        ).isoformat(),
                    }
                )
    return candidates


def archive_worktrees(
    codex_dir: Path,
    candidates: list[dict[str, Any]],
    stamp: str,
    dry_run: bool,
) -> list[dict[str, Any]]:
    archive_root = codex_dir / "archived_worktrees" / stamp
    moved: list[dict[str, Any]] = []
    for candidate in candidates:
        src = Path(candidate["path"])
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", rel(src, codex_dir))
        dst = unique_dest(archive_root / safe_name)
        if not dry_run:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
        moved.append({**candidate, "archived_to": str(dst)})
    return moved


def log_rotation_candidates(codex_dir: Path, threshold_bytes: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[Path] = set()

    for main in sorted(codex_dir.glob("logs_*.sqlite")):
        family = [main]
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(main) + suffix)
            if sidecar.exists():
                family.append(sidecar)
        total = sum(path.stat().st_size for path in family if path.exists())
        if total >= threshold_bytes:
            for path in family:
                seen.add(path)
            candidates.append(
                {
                    "kind": "sqlite_family",
                    "path": str(main),
                    "bytes": total,
                    "members": [str(path) for path in family],
                }
            )

    archive_dirs = {
        codex_dir / "archived_logs",
        codex_dir / "maintenance" / "reports",
        codex_dir / "maintenance" / "backups",
    }
    for path in sorted(codex_dir.rglob("*.log")):
        if any(parent in path.parents for parent in archive_dirs):
            continue
        if path in seen:
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size >= threshold_bytes:
            candidates.append(
                {"kind": "file", "path": str(path), "bytes": size, "members": [str(path)]}
            )
    return candidates


def rotate_logs(
    codex_dir: Path,
    candidates: list[dict[str, Any]],
    stamp: str,
    dry_run: bool,
) -> list[dict[str, Any]]:
    archive_root = codex_dir / "archived_logs" / stamp
    rotated: list[dict[str, Any]] = []
    for candidate in candidates:
        moved_members: list[dict[str, str]] = []
        for member in candidate["members"]:
            src = Path(member)
            if not src.exists():
                continue
            try:
                relative = src.relative_to(codex_dir)
            except ValueError:
                relative = Path(src.name)
            dst = unique_dest(archive_root / relative)
            if not dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
            moved_members.append({"from": str(src), "to": str(dst)})
        rotated.append({**candidate, "rotated_members": moved_members})
    return rotated


def iter_values(value: Any) -> Iterable[Any]:
    if isinstance(value, dict):
        for child in value.values():
            yield child
            yield from iter_values(child)
    elif isinstance(value, list):
        for child in value:
            yield child
            yield from iter_values(child)


def config_path_warnings(config_path: Path) -> list[str]:
    if not config_path.exists():
        return [f"missing config file: {config_path}"]
    try:
        data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"config could not be parsed: {exc}"]

    warnings: list[str] = []

    def walk(value: Any, dotted: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                next_key = f"{dotted}.{key}" if dotted else str(key)
                if dotted == "projects":
                    expanded = Path(str(key)).expanduser()
                    if expanded.is_absolute() and not expanded.exists():
                        warnings.append(f"{next_key}: missing project path {expanded}")
                walk(child, next_key)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{dotted}[{index}]")
        elif isinstance(value, str):
            if not looks_like_path(value):
                return
            expanded = Path(os.path.expandvars(value)).expanduser()
            if not expanded.is_absolute():
                expanded = (config_path.parent / expanded).resolve()
            if not expanded.exists():
                warnings.append(f"{dotted}: missing path {expanded}")

    walk(data, "")
    return warnings


def looks_like_path(value: str) -> bool:
    if value.startswith(("~", "/", "./", "../")):
        return True
    return "/Users/" in value or value.endswith((".app", ".sqlite", ".json", ".toml"))


def collect_workspace_paths(value: Any, key: str | None = None) -> Iterable[str]:
    if isinstance(value, dict):
        for child_key, child in value.items():
            yield from collect_workspace_paths(child, str(child_key))
    elif isinstance(value, list):
        for child in value:
            yield from collect_workspace_paths(child, key)
    elif isinstance(value, str) and key in WORKSPACE_KEYS:
        yield value


def bad_workspace_paths(codex_dir: Path, limit: int = 50) -> list[dict[str, str]]:
    roots = [codex_dir / "sessions", codex_dir / "archived_sessions"]
    bad: dict[str, str] = {}
    for root in roots:
        if not root.exists():
            continue
        for session in root.rglob("*.jsonl"):
            try:
                handle = session.open("r", encoding="utf-8", errors="replace")
            except OSError:
                continue
            with handle:
                for line in handle:
                    if '"cwd"' not in line and '"workspace' not in line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    for raw_path in collect_workspace_paths(record):
                        expanded = Path(raw_path).expanduser()
                        if not expanded.is_absolute():
                            continue
                        if not expanded.exists():
                            bad.setdefault(str(expanded), str(session))
                            if len(bad) >= limit:
                                return [
                                    {"path": path, "source": source}
                                    for path, source in sorted(bad.items())
                                ]
    return [{"path": path, "source": source} for path, source in sorted(bad.items())]


def summarize_area(codex_dir: Path) -> dict[str, Any]:
    sessions = codex_dir / "sessions"
    archived_sessions = codex_dir / "archived_sessions"
    archived_worktrees = codex_dir / "archived_worktrees"
    worktrees = [codex_dir / "worktrees", codex_dir / "tmp" / "worktrees", codex_dir / ".tmp" / "worktrees"]
    log_paths = [path for path in codex_dir.glob("logs*") if path.exists()]

    return {
        "sessions": {
            "bytes": disk_usage_bytes(sessions),
            "files": count_files(sessions, "*.jsonl"),
            "largest": largest_files(sessions, limit=8),
        },
        "archived_sessions": {
            "bytes": disk_usage_bytes(archived_sessions),
            "files": count_files(archived_sessions, "*.jsonl"),
            "largest": largest_files(archived_sessions, limit=8),
        },
        "worktrees": {
            "bytes": sum(disk_usage_bytes(path) for path in worktrees if path.exists()),
            "roots": [str(path) for path in worktrees if path.exists()],
            "archived_bytes": disk_usage_bytes(archived_worktrees),
        },
        "logs": {
            "bytes": sum(disk_usage_bytes(path) for path in log_paths),
            "files": count_files(codex_dir, "logs*"),
            "paths": [
                {"path": str(path), "bytes": disk_usage_bytes(path)}
                for path in sorted(log_paths)
            ],
        },
    }


def markdown_report(report: dict[str, Any]) -> str:
    area = report["final_space"]
    lines = [
        "# Codex Safe Cleanup Report",
        "",
        f"- created_at: {report['created_at']}",
        f"- codex_dir: {report['codex_dir']}",
        f"- backup_path: {report['backup_path']}",
        f"- mode: {report['mode']}",
        f"- cleanup_applied: {report['cleanup_applied']}",
        f"- codex_running: {report['codex_running']}",
        f"- disk_free: {human_bytes(report['disk_free_bytes'])}",
        "",
        "## Space",
        "",
        f"- sessions: {human_bytes(area['sessions']['bytes'])} ({area['sessions']['files']} files)",
        f"- archived_sessions: {human_bytes(area['archived_sessions']['bytes'])} ({area['archived_sessions']['files']} files)",
        f"- worktrees: {human_bytes(area['worktrees']['bytes'])}",
        f"- archived_worktrees: {human_bytes(area['worktrees']['archived_bytes'])}",
        f"- logs: {human_bytes(area['logs']['bytes'])}",
        "",
        "## Actions",
        "",
        f"- old_session_candidates: {len(report['old_session_candidates'])}",
        f"- archived_sessions: {len(report['archived_sessions'])}",
        f"- worktree_candidates: {len(report['worktree_candidates'])}",
        f"- worktrees_moved: {len(report['worktrees_moved'])}",
        f"- oversized_log_candidates: {len(report['log_rotation_candidates'])}",
        f"- logs_rotated: {len(report['logs_rotated'])}",
        "",
        "## Verification",
        "",
        f"- global_state_json_ok: {report['global_state_json']['ok']}",
        f"- state_sqlite_integrity_ok: {all(item.get('ok') for item in report['state_sqlite_integrity'])}",
        f"- config_path_warnings: {len(report['config_path_warnings'])}",
        f"- bad_workspace_paths: {len(report['bad_workspace_paths'])}",
        "",
    ]
    if report["config_path_warnings"]:
        lines.extend(["## Config Path Warnings", ""])
        lines.extend(f"- {warning}" for warning in report["config_path_warnings"])
        lines.append("")
    if report["bad_workspace_paths"]:
        lines.extend(["## Bad Workspace Paths", ""])
        lines.extend(
            f"- {item['path']} (from {item['source']})"
            for item in report["bad_workspace_paths"]
        )
        lines.append("")
    if report["backup"]["warnings"]:
        lines.extend(["## Backup Warnings", ""])
        lines.extend(f"- {warning}" for warning in report["backup"]["warnings"])
        lines.append("")
    return "\n".join(lines)


def print_short_report(report: dict[str, Any], report_path: Path) -> None:
    area = report["final_space"]
    print("Codex cleanup report")
    print(f"backup_path: {report['backup_path']}")
    print(f"report_path: {report_path}")
    print(f"mode: {report['mode']}")
    print(f"cleanup_applied: {report['cleanup_applied']}")
    print(
        "sessions: "
        f"{human_bytes(area['sessions']['bytes'])} ({area['sessions']['files']} files)"
    )
    print(
        "archived_sessions: "
        f"{human_bytes(area['archived_sessions']['bytes'])} "
        f"({area['archived_sessions']['files']} files)"
    )
    print(f"worktrees: {human_bytes(area['worktrees']['bytes'])}")
    print(f"logs: {human_bytes(area['logs']['bytes'])}")
    print(f"old_session_candidates: {len(report['old_session_candidates'])}")
    print(f"archived_sessions_this_run: {len(report['archived_sessions'])}")
    print(f"worktrees_moved: {len(report['worktrees_moved'])}")
    print(f"logs_rotated: {len(report['logs_rotated'])}")
    print(f"config_path_warnings: {len(report['config_path_warnings'])}")
    print(f"bad_workspace_paths: {len(report['bad_workspace_paths'])}")
    print(f"global_state_json_ok: {report['global_state_json']['ok']}")
    print(
        "state_sqlite_integrity_ok: "
        f"{all(item.get('ok') for item in report['state_sqlite_integrity'])}"
    )
    print(f"disk_free: {human_bytes(report['disk_free_bytes'])}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-dir", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--archive-age-days", type=int, default=10)
    parser.add_argument("--log-threshold-mb", type=int, default=100)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="inspect, back up, and report without moving any files",
    )
    args = parser.parse_args()

    codex_dir = args.codex_dir.expanduser().resolve()
    stamp = utc_stamp()
    maintenance_dir = codex_dir / "maintenance"
    backup_path = maintenance_dir / "backups" / stamp
    reports_dir = maintenance_dir / "reports"
    report_path = reports_dir / f"{stamp}-codex-cleanup.md"
    json_report_path = reports_dir / f"{stamp}-codex-cleanup.json"

    if not codex_dir.exists():
        print(f"Codex directory not found: {codex_dir}", file=sys.stderr)
        return 2

    initial_space = top_level_space(codex_dir)
    codex_processes = detect_codex_processes(Path(__file__).name)
    codex_running = bool(codex_processes)

    backup_path.mkdir(parents=True, exist_ok=False)
    backup = backup_important(codex_dir, backup_path)

    pinned_ids = collect_pinned_session_ids(codex_dir)
    old_candidates = old_session_candidates(codex_dir, args.archive_age_days, pinned_ids)
    worktree_candidates = find_worktree_candidates(codex_dir, args.archive_age_days)
    log_candidates = log_rotation_candidates(
        codex_dir,
        threshold_bytes=args.log_threshold_mb * 1024 * 1024,
    )

    cleanup_applied = bool(not codex_running and not args.dry_run and backup["critical_ok"])
    if codex_running:
        mode = "inspect-only (Codex is running)"
    elif args.dry_run:
        mode = "inspect-only (--dry-run)"
    elif not backup["critical_ok"]:
        mode = "inspect-only (backup warnings blocked cleanup)"
    else:
        mode = "cleanup-applied"

    archived_sessions: list[dict[str, Any]] = []
    worktrees_moved: list[dict[str, Any]] = []
    logs_rotated: list[dict[str, Any]] = []
    if cleanup_applied:
        archived_sessions = archive_old_sessions(codex_dir, old_candidates, stamp, False)
        worktrees_moved = archive_worktrees(codex_dir, worktree_candidates, stamp, False)
        logs_rotated = rotate_logs(codex_dir, log_candidates, stamp, False)

    global_state_path = codex_dir / ".codex-global-state.json"
    global_state_ok, global_state_error = parse_json_file(global_state_path)

    state_integrity: list[dict[str, Any]] = []
    state_backup_dir = backup_path / "sqlite"
    for state_db in sorted(state_backup_dir.glob("state_*.sqlite")):
        state_integrity.append(sqlite_integrity(state_db))
    if not state_integrity:
        state_integrity.append(
            {"path": str(state_backup_dir), "ok": False, "error": "no state database backup found"}
        )

    final_space = summarize_area(codex_dir)
    disk_free = shutil.disk_usage(codex_dir).free

    report = {
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "codex_dir": str(codex_dir),
        "backup_path": str(backup_path),
        "report_path": str(report_path),
        "json_report_path": str(json_report_path),
        "mode": mode,
        "cleanup_applied": cleanup_applied,
        "codex_running": codex_running,
        "codex_processes": codex_processes,
        "initial_top_level_space": initial_space,
        "final_space": final_space,
        "backup": backup,
        "pinned_session_ids": sorted(pinned_ids),
        "old_session_candidates": old_candidates,
        "archived_sessions": archived_sessions,
        "worktree_candidates": worktree_candidates,
        "worktrees_moved": worktrees_moved,
        "log_rotation_candidates": log_candidates,
        "logs_rotated": logs_rotated,
        "global_state_json": {"path": str(global_state_path), "ok": global_state_ok, "error": global_state_error},
        "state_sqlite_integrity": state_integrity,
        "config_path_warnings": config_path_warnings(codex_dir / "config.toml"),
        "bad_workspace_paths": bad_workspace_paths(codex_dir),
        "disk_free_bytes": disk_free,
        "archive_age_days": args.archive_age_days,
        "log_threshold_mb": args.log_threshold_mb,
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    json_report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    report_path.write_text(markdown_report(report), encoding="utf-8")

    print_short_report(report, report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
