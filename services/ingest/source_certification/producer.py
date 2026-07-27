"""Receipt-backed producer for source-certification evidence.

The release evaluator consumes one :class:`CertificationInput` per canonical
source.  This module is the deliberately stricter boundary that creates those
inputs:

* commands are declared in source-owned JSON binding files and run without a
  shell;
* only explicitly named environment variables reach a command;
* a real-provider canary cannot run without source-prefixed credential names;
* command output is accepted only through the strict ``CertificationInput``
  parser;
* every accepted artifact is a regular file beneath the stage artifact
  directory and is SHA-256 bound into a receipt;
* absent bindings, credentials, or entitlements produce blocked evidence, not
  passing placeholders.

The resulting directory has two independently uploaded parts:

``inputs/``
    Exactly 27 ``<source>.json`` files accepted by the release evaluator.

``provenance/``
    The producer manifest and every command/architecture receipt.  The
    consumer verifies this tree and the input hashes before evaluation.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.distributed_transport_diagnostic import (
    DISTRIBUTED_TRANSPORT_REDIS_ENV,
)
from services.ingest.source_certification.evaluator import evaluate_certification
from services.ingest.source_certification.io import (
    load_certification_input,
    parse_certification_input,
    write_certification_input,
)
from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    CertificationInvariantError,
    ScenarioResult,
    SourceCertificationSpec,
    SuiteResult,
)
from services.ingest.source_certification.stage_artifacts import (
    StageArtifactError,
    validate_stage_artifact,
)


PRODUCER_SCHEMA_VERSION = "fyralis.source-certification-producer.v1"
PRODUCER_SHARD_SCHEMA_VERSION = "fyralis.source-certification-producer-shard.v1"
BINDING_SCHEMA_VERSION = "fyralis.source-certification-execution-binding.v1"
RECEIPT_SCHEMA_VERSION = "fyralis.source-certification-command-receipt.v1"
_STAGES = ("local_correctness", "load", "fault_recovery", "canary")
_Stage = Literal["local_correctness", "load", "fault_recovery", "canary"]
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_ARTIFACT_PREFIX = "artifact://source-certification-evidence/"
_EVIDENCE_FILE_PREFIX = "evidence-file:"
_ARCHITECTURE_COUNT_RE = re.compile(r"\((\d+) new\)")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STAGE_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "commit_sha",
        "source_id",
        "stage",
        "state",
        "reason_code",
        "reason",
        "binding_path",
        "binding_sha256",
        "command",
        "required_environment",
        "credential_environment_names",
        "started_at",
        "completed_at",
        "returncode",
        "timed_out",
        "stdout_sha256",
        "stdout_bytes",
        "stderr_sha256",
        "stderr_bytes",
        "result_sha256",
        "artifact_sha256",
    }
)
_SAFE_ENV_NAMES = frozenset(
    {
        "CI",
        "COMPANY_OS_ENV",
        # Non-secret, evidence-labelled Provider Lab quota budgets. The load
        # driver treats this as optional: absent means provider-safe remains
        # blocked while the quota-disabled diagnostic can still run.
        "FYRALIS_PROVIDER_QUOTAS_JSON",
        "GITHUB_ACTIONS",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "LANG",
        "LC_ALL",
        "NO_PROXY",
        "PATH",
        "PYTHONPATH",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
        "TZ",
        "VIRTUAL_ENV",
    }
)
_LOCAL_PIPELINE_ENV_NAMES = frozenset(
    {
        # Optional local-correctness infrastructure.  These values may carry
        # database credentials, so they are forwarded only to the isolated
        # local stage and are never serialized into receipts.  The execution
        # driver additionally requires the explicit loopback-only ACK.
        "FYRALIS_CERTIFICATION_DATABASE_URL",
        "FYRALIS_CERTIFICATION_ISOLATED_INFRA_ACK",
        "FYRALIS_CERTIFICATION_KAFKA_BOOTSTRAP_SERVERS",
        "FYRALIS_CERTIFICATION_S3_ENDPOINT_URL",
        "FYRALIS_CERTIFICATION_S3_RAW_BUCKET",
    }
)
_SENSITIVE_RUNTIME_ENV_NAMES = frozenset(
    {
        "FYRALIS_CERTIFICATION_DATABASE_URL",
        DISTRIBUTED_TRANSPORT_REDIS_ENV,
    }
)
_SECRET_NAME_RE = re.compile(
    r"(?:API_?KEY|CREDENTIAL|PASSWORD|PRIVATE_?KEY|SECRET|TOKEN)"
)
_MIN_SECRET_BYTES = 8


class EvidenceProducerError(CertificationInvariantError):
    """The producer configuration or evidence bundle is invalid."""


@dataclass(frozen=True, slots=True)
class StageCommand:
    argv: tuple[str, ...]
    timeout_seconds: int
    required_env: tuple[str, ...]
    credential_env: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExecutionBinding:
    source_id: str
    spec_hash: str
    path: Path
    relative_path: str
    sha256: str
    stages: Mapping[str, StageCommand | None]


@dataclass(frozen=True, slots=True)
class RepositoryIdentity:
    commit_sha: str
    head_sha: str
    clean: bool
    status_sha256: str
    status_entry_count: int


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    returncode: int
    started_at: datetime
    completed_at: datetime
    stdout: bytes
    stderr: bytes
    timed_out: bool = False
    launch_error: str | None = None


CommandExecutor = Callable[
    [Sequence[str], Path, Mapping[str, str], int],
    CommandOutcome,
]


@dataclass(frozen=True, slots=True)
class StageProduct:
    state: str
    reason: str
    receipt_path: str
    receipt_sha256: str
    supplied: CertificationInput | None
    artifact_uri_map: Mapping[str, str]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def certification_catalog_identity() -> tuple[dict[str, str], str]:
    """Return the ordered spec hashes and their canonical catalog digest."""

    spec_hashes = {
        source_id: spec.declaration_hash()
        for source_id, spec in SOURCE_CERTIFICATION_CATALOG.items()
    }
    digest = _sha256_bytes(
        _canonical_json(
            {
                "source_order": list(SOURCE_CERTIFICATION_CATALOG),
                "spec_hashes": spec_hashes,
            }
        )
    )
    return spec_hashes, digest


def deterministic_source_shard(
    shard_index: int,
    shard_count: int,
) -> tuple[str, ...]:
    """Select a stable round-robin shard from canonical catalog order."""

    source_order = tuple(SOURCE_CERTIFICATION_CATALOG)
    if (
        isinstance(shard_index, bool)
        or isinstance(shard_count, bool)
        or not isinstance(shard_index, int)
        or not isinstance(shard_count, int)
        or shard_count < 1
        or shard_count > len(source_order)
        or shard_index < 0
        or shard_index >= shard_count
    ):
        raise EvidenceProducerError(
            "shard_index/shard_count must select a non-empty canonical shard"
        )
    selected = source_order[shard_index::shard_count]
    if not selected:
        raise EvidenceProducerError("source shard must not be empty")
    return selected


def _atomic_write_json(path: Path, value: object) -> str:
    rendered = _canonical_json(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return _sha256_bytes(rendered)


def _load_unique_json(path: Path) -> object:
    def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise EvidenceProducerError(
                    f"{path} contains duplicate JSON key {key!r}"
                )
            result[key] = value
        return result

    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_pairs,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceProducerError(f"cannot load {path}: {exc}") from exc


def _allowed_secret_bundle_name(
    name: str,
    *,
    source_id: str | None,
) -> bool:
    if (
        name in _LOCAL_PIPELINE_ENV_NAMES
        or name == DISTRIBUTED_TRANSPORT_REDIS_ENV
        or name == "FYRALIS_PROVIDER_QUOTAS_JSON"
    ):
        return True
    specs = (
        (SOURCE_CERTIFICATION_CATALOG[source_id],)
        if source_id is not None
        else tuple(SOURCE_CERTIFICATION_CATALOG.values())
    )
    return any(
        name == spec.canary.credential_env_prefix
        or name.startswith(f"{spec.canary.credential_env_prefix}_")
        for spec in specs
    )


def load_secret_environment_bundle(
    path: Path,
    *,
    source_id: str | None = None,
) -> dict[str, str]:
    """Load an exact mode-0600 environment bundle without exporting values.

    The workflow passes only this file path to the producer.  Secret values
    remain process-local and are never written to ``GITHUB_ENV``, command
    receipts, or producer manifests.
    """

    if (
        source_id is not None
        and source_id not in SOURCE_CERTIFICATION_CATALOG
    ):
        raise EvidenceProducerError(
            "secret environment bundle source is not canonical"
        )
    if path.is_symlink() or not path.is_file():
        raise EvidenceProducerError(
            "secret environment bundle must be a regular file"
        )
    try:
        metadata = path.stat()
    except OSError as exc:
        raise EvidenceProducerError(
            f"cannot inspect secret environment bundle: {exc}"
        ) from exc
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise EvidenceProducerError(
            "secret environment bundle permissions must be exactly 0600"
        )
    if hasattr(os, "geteuid") and metadata.st_uid != os.geteuid():
        raise EvidenceProducerError(
            "secret environment bundle must be owned by the producer user"
        )
    raw = _load_unique_json(path)
    if not isinstance(raw, Mapping):
        raise EvidenceProducerError(
            "secret environment bundle must be a JSON object"
        )
    result: dict[str, str] = {}
    for name, value in raw.items():
        if (
            not isinstance(name, str)
            or _ENV_NAME_RE.fullmatch(name) is None
            or not _allowed_secret_bundle_name(name, source_id=source_id)
        ):
            raise EvidenceProducerError(
                "secret environment bundle contains an unsupported "
                "environment name"
            )
        if not isinstance(value, str) or not value:
            raise EvidenceProducerError(
                f"secret environment bundle value for {name} must be non-empty"
            )
        result[name] = value
    return result


def _json_secret_strings(value: object) -> tuple[str, ...]:
    values: list[str] = []

    def visit(item: object) -> None:
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, Mapping):
            for child in item.values():
                visit(child)
        elif isinstance(item, Sequence) and not isinstance(
            item,
            (str, bytes, bytearray),
        ):
            for child in item:
                visit(child)

    visit(value)
    return tuple(values)


def _secret_needles(
    command: StageCommand,
    *,
    environment: Mapping[str, str],
    stage: str,
) -> tuple[bytes, ...]:
    names = set(command.credential_env) | _SENSITIVE_RUNTIME_ENV_NAMES
    if stage == "canary":
        names.update(command.required_env)
    names.update(
        name
        for name in command.required_env
        if _SECRET_NAME_RE.search(name) is not None
    )
    candidates: list[str] = []
    for name in sorted(names):
        raw = environment.get(name)
        if not raw:
            continue
        candidates.append(raw)
        try:
            parsed_url = urlsplit(raw)
        except ValueError:
            parsed_url = None
        if parsed_url is not None and parsed_url.password:
            candidates.append(parsed_url.password)
        try:
            decoded = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            pass
        else:
            candidates.extend(_json_secret_strings(decoded))
    return tuple(
        sorted(
            {
                encoded
                for value in candidates
                if len(encoded := value.encode("utf-8")) >= _MIN_SECRET_BYTES
            },
            key=lambda value: (-len(value), value),
        )
    )


def _bytes_contain_secret(value: bytes, needles: Sequence[bytes]) -> bool:
    return any(needle in value for needle in needles)


def _file_contains_secret(path: Path, needles: Sequence[bytes]) -> bool:
    if not needles:
        return False
    maximum = max(len(needle) for needle in needles)
    tail = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            window = tail + chunk
            if _bytes_contain_secret(window, needles):
                return True
            tail = window[-(maximum - 1) :] if maximum > 1 else b""
    return False


def _stage_output_contains_secret(
    *,
    outcome: CommandOutcome,
    result_path: Path,
    artifact_dir: Path,
    needles: Sequence[bytes],
) -> bool:
    if not needles:
        return False
    if _bytes_contain_secret(outcome.stdout, needles) or _bytes_contain_secret(
        outcome.stderr,
        needles,
    ):
        return True
    if (
        not result_path.is_symlink()
        and result_path.is_file()
        and _file_contains_secret(result_path, needles)
    ):
        return True
    if not artifact_dir.is_dir() or artifact_dir.is_symlink():
        return False
    for path in artifact_dir.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        if _file_contains_secret(path, needles):
            return True
    return False


def _expected_plan_sha256(argv: Sequence[str]) -> str:
    indexes = [
        index
        for index, value in enumerate(argv)
        if value == "--plan-sha256"
    ]
    if len(indexes) != 1 or indexes[0] + 1 >= len(argv):
        raise EvidenceProducerError(
            "stage command must bind exactly one --plan-sha256 value"
        )
    value = argv[indexes[0] + 1]
    if _SHA256_RE.fullmatch(value) is None:
        raise EvidenceProducerError(
            "stage command --plan-sha256 value is invalid"
        )
    return value


def _receipt_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceProducerError(f"{field} must be an ISO-8601 timestamp")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise EvidenceProducerError(
            f"{field} must be a valid ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvidenceProducerError(f"{field} must be timezone-aware")
    return parsed


def _exact_keys(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    field: str,
) -> None:
    missing = expected - value.keys()
    extra = value.keys() - expected
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {sorted(missing)!r}")
        if extra:
            details.append(f"unknown {sorted(extra)!r}")
        raise EvidenceProducerError(f"{field} fields are invalid: {', '.join(details)}")


def _tuple_of_strings(value: object, *, field: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise EvidenceProducerError(f"{field} must be an array of non-empty strings")
    if len(value) != len(set(value)):
        raise EvidenceProducerError(f"{field} must not contain duplicates")
    return tuple(value)


def _parse_stage_command(
    value: object,
    *,
    source_id: str,
    stage: str,
    credential_prefix: str,
) -> StageCommand | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EvidenceProducerError(
            f"{source_id}.{stage} binding must be an object or null"
        )
    _exact_keys(
        value,
        frozenset(
            {
                "argv",
                "timeout_seconds",
                "required_env",
                "credential_env",
            }
        ),
        field=f"{source_id}.{stage}",
    )
    argv = _tuple_of_strings(value["argv"], field=f"{source_id}.{stage}.argv")
    timeout = value["timeout_seconds"]
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise EvidenceProducerError(
            f"{source_id}.{stage}.timeout_seconds must be a positive integer"
        )
    if timeout > 21_600:
        raise EvidenceProducerError(
            f"{source_id}.{stage}.timeout_seconds cannot exceed six hours"
        )
    required_env = _tuple_of_strings(
        value["required_env"],
        field=f"{source_id}.{stage}.required_env",
    )
    credential_env = _tuple_of_strings(
        value["credential_env"],
        field=f"{source_id}.{stage}.credential_env",
    )
    for name in (*required_env, *credential_env):
        if _ENV_NAME_RE.fullmatch(name) is None:
            raise EvidenceProducerError(
                f"{source_id}.{stage} has invalid environment name {name!r}"
            )
    if not set(credential_env).issubset(required_env):
        raise EvidenceProducerError(
            f"{source_id}.{stage}.credential_env must be a subset of required_env"
        )
    if stage == "canary":
        accepted_prefix = f"{credential_prefix}_"
        foreign_canary_names = [
            name
            for name in required_env
            if name.startswith("FYRALIS_CANARY_")
            and name != credential_prefix
            and not name.startswith(accepted_prefix)
        ]
        if foreign_canary_names:
            raise EvidenceProducerError(
                f"{source_id}.canary cannot receive another source's credentials: "
                + ", ".join(foreign_canary_names)
            )
        if not credential_env or not all(
            name == credential_prefix or name.startswith(accepted_prefix)
            for name in credential_env
        ):
            raise EvidenceProducerError(
                f"{source_id}.canary credential_env must contain only "
                f"{credential_prefix!r}-prefixed names"
            )
    else:
        canary_names = [
            name for name in required_env if name.startswith("FYRALIS_CANARY_")
        ]
        if credential_env or canary_names:
            raise EvidenceProducerError(
                f"{source_id}.{stage} must not receive real-provider credentials"
            )
    return StageCommand(
        argv=argv,
        timeout_seconds=timeout,
        required_env=required_env,
        credential_env=credential_env,
    )


def load_execution_binding(
    path: Path,
    *,
    repo_root: Path,
    spec: SourceCertificationSpec,
) -> ExecutionBinding:
    """Load one exact, source-owned command binding."""

    if path.is_symlink() or not path.is_file():
        raise EvidenceProducerError(f"execution binding must be a regular file: {path}")
    try:
        relative_path = path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError as exc:
        raise EvidenceProducerError(
            f"execution binding must live beneath the repository: {path}"
        ) from exc
    raw = _load_unique_json(path)
    if not isinstance(raw, Mapping):
        raise EvidenceProducerError(f"execution binding {path} must be an object")
    _exact_keys(
        raw,
        frozenset({"schema_version", "source_id", "spec_hash", "stages"}),
        field=str(path),
    )
    if raw["schema_version"] != BINDING_SCHEMA_VERSION:
        raise EvidenceProducerError(
            f"{path} schema_version must equal {BINDING_SCHEMA_VERSION!r}"
        )
    if raw["source_id"] != spec.source_id:
        raise EvidenceProducerError(
            f"{path} source_id must equal {spec.source_id!r}"
        )
    if raw["spec_hash"] != spec.declaration_hash():
        raise EvidenceProducerError(f"{path} spec_hash is stale")
    stages = raw["stages"]
    if not isinstance(stages, Mapping):
        raise EvidenceProducerError(f"{path}.stages must be an object")
    _exact_keys(stages, frozenset(_STAGES), field=f"{path}.stages")
    parsed = {
        stage: _parse_stage_command(
            stages[stage],
            source_id=spec.source_id,
            stage=stage,
            credential_prefix=spec.canary.credential_env_prefix,
        )
        for stage in _STAGES
    }
    return ExecutionBinding(
        source_id=spec.source_id,
        spec_hash=spec.declaration_hash(),
        path=path,
        relative_path=relative_path,
        sha256=_sha256_file(path),
        stages=parsed,
    )


def _load_bindings(
    binding_dir: Path,
    *,
    repo_root: Path,
) -> tuple[dict[str, ExecutionBinding], dict[str, str]]:
    bindings: dict[str, ExecutionBinding] = {}
    errors: dict[str, str] = {}
    if not binding_dir.exists():
        return bindings, errors
    if binding_dir.is_symlink() or not binding_dir.is_dir():
        raise EvidenceProducerError(
            f"execution binding directory must be a regular directory: {binding_dir}"
        )
    expected = {
        f"{source_id}.json" for source_id in SOURCE_CERTIFICATION_CATALOG
    }
    unexpected = sorted(
        entry.name
        for entry in binding_dir.iterdir()
        if entry.name not in expected
    )
    if unexpected:
        raise EvidenceProducerError(
            "execution binding directory contains unexpected entries: "
            + ", ".join(unexpected)
        )
    for source_id, spec in SOURCE_CERTIFICATION_CATALOG.items():
        path = binding_dir / f"{source_id}.json"
        if not path.exists():
            continue
        try:
            bindings[source_id] = load_execution_binding(
                path,
                repo_root=repo_root,
                spec=spec,
            )
        except EvidenceProducerError as exc:
            errors[source_id] = str(exc)
    return bindings, errors


def inspect_repository(
    repo_root: Path,
    *,
    expected_commit_sha: str | None = None,
) -> RepositoryIdentity:
    """Resolve exact git identity without treating a dirty tree as certifiable."""

    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            check=True,
            timeout=15,
        ).stdout.decode("ascii").strip()
        status = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=repo_root,
            capture_output=True,
            check=True,
            timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError) as exc:
        raise EvidenceProducerError(f"cannot inspect repository identity: {exc}") from exc
    commit = expected_commit_sha or head
    if _COMMIT_RE.fullmatch(commit) is None:
        raise EvidenceProducerError(
            "commit SHA must be a lowercase full 40-character digest"
        )
    if _COMMIT_RE.fullmatch(head) is None:
        raise EvidenceProducerError("git HEAD is not a lowercase full commit SHA")
    return RepositoryIdentity(
        commit_sha=commit,
        head_sha=head,
        clean=not status and head == commit,
        status_sha256=_sha256_bytes(status),
        status_entry_count=len(status.splitlines()),
    )


def _default_executor(
    argv: Sequence[str],
    cwd: Path,
    env: Mapping[str, str],
    timeout_seconds: int,
) -> CommandOutcome:
    started = datetime.now(timezone.utc)
    try:
        completed = subprocess.run(
            list(argv),
            cwd=cwd,
            env=dict(env),
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return CommandOutcome(
            returncode=completed.returncode,
            started_at=started,
            completed_at=datetime.now(timezone.utc),
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, bytes) else b""
        stderr = exc.stderr if isinstance(exc.stderr, bytes) else b""
        return CommandOutcome(
            returncode=124,
            started_at=started,
            completed_at=datetime.now(timezone.utc),
            stdout=stdout,
            stderr=stderr,
            timed_out=True,
            launch_error="command timed out",
        )
    except OSError as exc:
        return CommandOutcome(
            returncode=127,
            started_at=started,
            completed_at=datetime.now(timezone.utc),
            stdout=b"",
            stderr=b"",
            launch_error=f"{type(exc).__name__}: {exc}",
        )


def _command_environment(
    command: StageCommand,
    *,
    ambient: Mapping[str, str],
    source_id: str,
    stage: str,
    result_path: Path,
    artifact_dir: Path,
    commit_sha: str,
) -> tuple[dict[str, str], dict[str, bool]]:
    presence = {
        name: bool(ambient.get(name))
        for name in command.required_env
    }
    selected = {
        name: value
        for name in _SAFE_ENV_NAMES
        if (value := ambient.get(name))
    }
    if (
        stage in {"load", "fault_recovery"}
        and (redis_url := ambient.get(DISTRIBUTED_TRANSPORT_REDIS_ENV))
    ):
        # This URL can contain credentials. Pass it only to the two stages
        # that execute the bounded shared-Redis diagnostic; never serialize
        # it into the command receipt or expose it to local/canary commands.
        selected[DISTRIBUTED_TRANSPORT_REDIS_ENV] = redis_url
    if stage == "local_correctness":
        selected.update(
            {
                name: value
                for name in _LOCAL_PIPELINE_ENV_NAMES
                if (value := ambient.get(name))
            }
        )
    for name in command.required_env:
        value = ambient.get(name)
        if value:
            selected[name] = value
    selected.update(
        {
            "FYRALIS_CERTIFICATION_ARTIFACT_DIR": str(artifact_dir),
            "FYRALIS_CERTIFICATION_COMMIT_SHA": commit_sha,
            "FYRALIS_CERTIFICATION_RESULT_PATH": str(result_path),
            "FYRALIS_CERTIFICATION_SOURCE_ID": source_id,
            "FYRALIS_CERTIFICATION_STAGE": stage,
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return selected, presence


def _expand_argv(
    command: StageCommand,
    *,
    repo_root: Path,
    source_id: str,
    stage: str,
    result_path: Path,
    artifact_dir: Path,
) -> tuple[str, ...]:
    values = {
        "artifact_dir": str(artifact_dir),
        "python": sys.executable,
        "repo_root": str(repo_root),
        "result_path": str(result_path),
        "source_id": source_id,
        "stage": stage,
    }
    expanded = []
    for argument in command.argv:
        try:
            rendered = argument.format_map(values)
        except (KeyError, ValueError) as exc:
            raise EvidenceProducerError(
                f"invalid command placeholder in {argument!r}: {exc}"
            ) from exc
        if "\x00" in rendered or "\n" in rendered or "\r" in rendered:
            raise EvidenceProducerError("command arguments cannot contain control lines")
        expanded.append(rendered)
    return tuple(expanded)


def _artifact_uri(run_id: str, relative_path: str, sha256: str) -> str:
    return (
        f"{_ARTIFACT_PREFIX}{run_id}/{relative_path}"
        f"#sha256={sha256}"
    )


def _receipt_uri(run_id: str, path: str, sha256: str) -> str:
    return _artifact_uri(run_id, path, sha256)


def _blocked_input(
    spec: SourceCertificationSpec,
    *,
    stage_products: Mapping[str, StageProduct],
    legacy_reference_count: int,
) -> CertificationInput:
    local = stage_products["local_correctness"]
    load = stage_products["load"]
    recovery = stage_products["fault_recovery"]
    canary = stage_products["canary"]

    local_uri = _receipt_uri(
        _run_id_from_uri_parts(local),
        local.receipt_path,
        local.receipt_sha256,
    )
    load_uri = _receipt_uri(
        _run_id_from_uri_parts(load),
        load.receipt_path,
        load.receipt_sha256,
    )
    recovery_uri = _receipt_uri(
        _run_id_from_uri_parts(recovery),
        recovery.receipt_path,
        recovery.receipt_sha256,
    )
    canary_uri = _receipt_uri(
        _run_id_from_uri_parts(canary),
        canary.receipt_path,
        canary.receipt_sha256,
    )

    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness="blocked",
        local_correctness_artifact=local_uri,
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state="blocked",
                artifact_uri=local_uri,
                failures=(local.reason,),
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=tuple(
            SuiteResult(
                kind=suite.kind,
                state="blocked",
                artifact_uri=load_uri,
                failures=(load.reason,),
            )
            for suite in spec.load_suites
        ),
        fyralis_ceiling_suites=tuple(
            SuiteResult(
                kind=suite.kind,
                state="blocked",
                artifact_uri=load_uri,
                failures=(load.reason,),
            )
            for suite in spec.load_suites
        ),
        fault_recovery_suites=tuple(
            SuiteResult(
                kind=suite.kind,
                state="blocked",
                artifact_uri=recovery_uri,
                failures=(recovery.reason,),
            )
            for suite in spec.load_suites
        ),
        canary=CanaryResult(
            state="blocked",
            operation_results=tuple(
                CanaryOperationResult(
                    operation_id=operation_id,
                    state="blocked",
                    artifact_uri=canary_uri,
                    failures=(canary.reason,),
                )
                for operation_id in spec.canary.required_operations
            ),
            artifact_uri=canary_uri,
            failures=(canary.reason,),
        ),
        legacy_reference_count=legacy_reference_count,
    )


def _run_id_from_uri_parts(product: StageProduct) -> str:
    # The run ID is embedded in the producer-populated receipt path marker.
    # ``receipt_path`` itself is relative; the temporary private marker is
    # stored in ``artifact_uri_map`` and never serialized as evidence.
    return product.artifact_uri_map["__run_id__"]


def _replace_evidence_uri(
    uri: str | None,
    *,
    artifact_map: Mapping[str, str],
    field: str,
    required: bool,
) -> str | None:
    if uri is None:
        if required:
            raise EvidenceProducerError(f"{field} artifact is missing")
        return None
    if not uri.startswith(_EVIDENCE_FILE_PREFIX):
        raise EvidenceProducerError(
            f"{field} must use {_EVIDENCE_FILE_PREFIX!r}, got {uri!r}"
        )
    relative = uri[len(_EVIDENCE_FILE_PREFIX) :]
    try:
        return artifact_map[relative]
    except KeyError as exc:
        raise EvidenceProducerError(
            f"{field} references undeclared stage artifact {relative!r}"
        ) from exc


def _translate_local(
    base: CertificationInput,
    supplied: CertificationInput,
    artifact_map: Mapping[str, str],
) -> CertificationInput:
    scenarios = tuple(
        dataclasses.replace(
            result,
            artifact_uri=cast(
                str,
                _replace_evidence_uri(
                    result.artifact_uri,
                    artifact_map=artifact_map,
                    field=f"scenario.{result.scenario_id}",
                    required=True,
                ),
            ),
        )
        for result in supplied.scenario_results
    )
    return dataclasses.replace(
        base,
        local_correctness=supplied.local_correctness,
        local_correctness_artifact=_replace_evidence_uri(
            supplied.local_correctness_artifact,
            artifact_map=artifact_map,
            field="local_correctness",
            required=supplied.local_correctness == "passed",
        ),
        scenario_results=scenarios,
        skipped_tests=tuple(
            dict.fromkeys((*base.skipped_tests, *supplied.skipped_tests))
        ),
        todos=tuple(dict.fromkeys((*base.todos, *supplied.todos))),
    )


def _translate_suites(
    suites: tuple[SuiteResult, ...],
    *,
    artifact_map: Mapping[str, str],
    field: str,
) -> tuple[SuiteResult, ...]:
    return tuple(
        dataclasses.replace(
            suite,
            artifact_uri=_replace_evidence_uri(
                suite.artifact_uri,
                artifact_map=artifact_map,
                field=f"{field}.{suite.kind}",
                required=suite.state == "passed",
            ),
        )
        for suite in suites
    )


def _translate_load(
    base: CertificationInput,
    supplied: CertificationInput,
    artifact_map: Mapping[str, str],
) -> CertificationInput:
    return dataclasses.replace(
        base,
        provider_safe_suites=_translate_suites(
            supplied.provider_safe_suites,
            artifact_map=artifact_map,
            field="provider_safe",
        ),
        fyralis_ceiling_suites=_translate_suites(
            supplied.fyralis_ceiling_suites,
            artifact_map=artifact_map,
            field="fyralis_ceiling",
        ),
        skipped_tests=tuple(
            dict.fromkeys((*base.skipped_tests, *supplied.skipped_tests))
        ),
        todos=tuple(dict.fromkeys((*base.todos, *supplied.todos))),
    )


def _translate_recovery(
    base: CertificationInput,
    supplied: CertificationInput,
    artifact_map: Mapping[str, str],
) -> CertificationInput:
    return dataclasses.replace(
        base,
        fault_recovery_suites=_translate_suites(
            supplied.fault_recovery_suites,
            artifact_map=artifact_map,
            field="fault_recovery",
        ),
        skipped_tests=tuple(
            dict.fromkeys((*base.skipped_tests, *supplied.skipped_tests))
        ),
        todos=tuple(dict.fromkeys((*base.todos, *supplied.todos))),
    )


def _translate_canary(
    base: CertificationInput,
    supplied: CertificationInput,
    artifact_map: Mapping[str, str],
) -> CertificationInput:
    canary = dataclasses.replace(
        supplied.canary,
        artifact_uri=_replace_evidence_uri(
            supplied.canary.artifact_uri,
            artifact_map=artifact_map,
            field="canary",
            required=supplied.canary.state == "passed",
        ),
        operation_results=tuple(
            dataclasses.replace(
                result,
                artifact_uri=cast(
                    str,
                    _replace_evidence_uri(
                        result.artifact_uri,
                        artifact_map=artifact_map,
                        field=f"canary.{result.operation_id}",
                        required=True,
                    ),
                ),
            )
            for result in supplied.canary.operation_results
        ),
    )
    return dataclasses.replace(
        base,
        canary=canary,
        skipped_tests=tuple(
            dict.fromkeys((*base.skipped_tests, *supplied.skipped_tests))
        ),
        todos=tuple(dict.fromkeys((*base.todos, *supplied.todos))),
    )


def _apply_stage(
    base: CertificationInput,
    *,
    stage: str,
    product: StageProduct,
) -> CertificationInput:
    supplied = product.supplied
    if supplied is None:
        return base
    if stage == "local_correctness":
        return _translate_local(base, supplied, product.artifact_uri_map)
    if stage == "load":
        return _translate_load(base, supplied, product.artifact_uri_map)
    if stage == "fault_recovery":
        return _translate_recovery(base, supplied, product.artifact_uri_map)
    return _translate_canary(base, supplied, product.artifact_uri_map)


def _stage_state(stage: str, supplied: CertificationInput) -> str:
    if stage == "local_correctness":
        states = [
            supplied.local_correctness,
            *(result.state for result in supplied.scenario_results),
        ]
    elif stage == "load":
        states = [
            *(result.state for result in supplied.provider_safe_suites),
            *(result.state for result in supplied.fyralis_ceiling_suites),
        ]
    elif stage == "fault_recovery":
        states = [result.state for result in supplied.fault_recovery_suites]
    else:
        states = [
            supplied.canary.state,
            *(result.state for result in supplied.canary.operation_results),
        ]
    if states and all(state == "passed" for state in states):
        return "passed"
    if any(state == "failed" for state in states):
        return "failed"
    return "blocked"


def _collect_stage_artifacts(
    artifact_dir: Path,
    *,
    output_root: Path,
    run_id: str,
) -> tuple[dict[str, str], dict[str, str]]:
    uri_map: dict[str, str] = {"__run_id__": run_id}
    file_hashes: dict[str, str] = {}
    if not artifact_dir.exists():
        return uri_map, file_hashes
    if artifact_dir.is_symlink() or not artifact_dir.is_dir():
        raise EvidenceProducerError(
            f"stage artifact root must be a regular directory: {artifact_dir}"
        )
    for path in sorted(artifact_dir.rglob("*")):
        if path.is_symlink():
            raise EvidenceProducerError(
                f"stage artifact must not be a symlink: {path}"
            )
        if path.is_dir():
            continue
        if not path.is_file():
            raise EvidenceProducerError(
                f"stage artifact must be a regular file: {path}"
            )
        relative_to_artifacts = path.relative_to(artifact_dir).as_posix()
        pure = PurePosixPath(relative_to_artifacts)
        if pure.is_absolute() or ".." in pure.parts:
            raise EvidenceProducerError(
                f"stage artifact path escapes its directory: {path}"
            )
        relative_to_output = path.relative_to(output_root).as_posix()
        digest = _sha256_file(path)
        file_hashes[relative_to_output] = digest
        uri_map[relative_to_artifacts] = _artifact_uri(
            run_id,
            relative_to_output,
            digest,
        )
    return uri_map, file_hashes


def _validate_typed_stage_artifact(
    artifact_dir: Path,
    *,
    spec: SourceCertificationSpec,
    stage: str,
    supplied: CertificationInput,
    started_at: datetime,
    completed_at: datetime,
    argv: Sequence[str],
) -> None:
    stage_path = artifact_dir / "stage.json"
    if stage_path.is_symlink() or not stage_path.is_file():
        raise EvidenceProducerError(
            f"{stage} command did not emit a regular typed stage.json artifact"
        )
    try:
        validate_stage_artifact(
            _load_unique_json(stage_path),
            spec=spec,
            stage=stage,  # type: ignore[arg-type]
            supplied=supplied,
            started_at=started_at,
            completed_at=completed_at,
            expected_plan_sha256=_expected_plan_sha256(argv),
        )
    except StageArtifactError as exc:
        raise EvidenceProducerError(str(exc)) from exc


def _write_receipt(
    provenance_root: Path,
    *,
    relative_path: str,
    value: Mapping[str, Any],
) -> tuple[str, str]:
    path = provenance_root.parent / relative_path
    digest = _atomic_write_json(path, value)
    return relative_path, digest


def _missing_stage_product(
    *,
    output_root: Path,
    provenance_root: Path,
    run_id: str,
    commit_sha: str,
    source_id: str,
    stage: str,
    reason_code: str,
    reason: str,
    binding: ExecutionBinding | None,
    required_environment: Mapping[str, bool] | None = None,
    credential_environment_names: Sequence[str] = (),
) -> StageProduct:
    relative = f"provenance/receipts/{source_id}/{stage}/receipt.json"
    value = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": run_id,
        "commit_sha": commit_sha,
        "source_id": source_id,
        "stage": stage,
        "state": "blocked",
        "reason_code": reason_code,
        "reason": reason,
        "binding_path": binding.relative_path if binding else None,
        "binding_sha256": binding.sha256 if binding else None,
        "command": None,
        "required_environment": dict(required_environment or {}),
        "credential_environment_names": list(credential_environment_names),
        "started_at": None,
        "completed_at": None,
        "returncode": None,
        "timed_out": False,
        "stdout_sha256": None,
        "stdout_bytes": 0,
        "stderr_sha256": None,
        "stderr_bytes": 0,
        "result_sha256": None,
        "artifact_sha256": {},
    }
    receipt_path, receipt_sha = _write_receipt(
        provenance_root,
        relative_path=relative,
        value=value,
    )
    return StageProduct(
        state="blocked",
        reason=reason,
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha,
        supplied=None,
        artifact_uri_map={"__run_id__": run_id},
    )


def _execute_stage(
    *,
    output_root: Path,
    provenance_root: Path,
    repo_root: Path,
    run_id: str,
    commit_sha: str,
    spec: SourceCertificationSpec,
    stage: str,
    binding: ExecutionBinding,
    ambient_env: Mapping[str, str],
    executor: CommandExecutor,
) -> StageProduct:
    command = binding.stages[stage]
    if command is None:
        return _missing_stage_product(
            output_root=output_root,
            provenance_root=provenance_root,
            run_id=run_id,
            commit_sha=commit_sha,
            source_id=spec.source_id,
            stage=stage,
            reason_code="executable_binding_absent",
            reason=f"{stage} executable binding is absent",
            binding=binding,
        )
    stage_dir = provenance_root / "receipts" / spec.source_id / stage
    artifact_dir = stage_dir / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    result_path = stage_dir / ".command-result.json"
    env, presence = _command_environment(
        command,
        ambient=ambient_env,
        source_id=spec.source_id,
        stage=stage,
        result_path=result_path,
        artifact_dir=artifact_dir,
        commit_sha=commit_sha,
    )
    missing = sorted(name for name, present in presence.items() if not present)
    if missing:
        return _missing_stage_product(
            output_root=output_root,
            provenance_root=provenance_root,
            run_id=run_id,
            commit_sha=commit_sha,
            source_id=spec.source_id,
            stage=stage,
            reason_code=(
                "canary_credentials_or_entitlements_absent"
                if stage == "canary"
                else "required_environment_absent"
            ),
            reason=(
                f"{stage} required environment is absent: "
                + ", ".join(missing)
            ),
            binding=binding,
            required_environment=presence,
            credential_environment_names=command.credential_env,
        )
    try:
        argv = _expand_argv(
            command,
            repo_root=repo_root,
            source_id=spec.source_id,
            stage=stage,
            result_path=result_path,
            artifact_dir=artifact_dir,
        )
    except EvidenceProducerError as exc:
        return _missing_stage_product(
            output_root=output_root,
            provenance_root=provenance_root,
            run_id=run_id,
            commit_sha=commit_sha,
            source_id=spec.source_id,
            stage=stage,
            reason_code="invalid_executable_binding",
            reason=str(exc),
            binding=binding,
        )
    outcome = executor(argv, repo_root, env, command.timeout_seconds)
    supplied: CertificationInput | None = None
    result_sha: str | None = None
    result_error: str | None = None
    artifact_map: dict[str, str]
    artifact_hashes: dict[str, str]
    leaked_output = _stage_output_contains_secret(
        outcome=outcome,
        result_path=result_path,
        artifact_dir=artifact_dir,
        needles=_secret_needles(
            command,
            environment=env,
            stage=stage,
        ),
    )
    if leaked_output:
        result_error = (
            "stage output contained a provided credential value; all command "
            "output and artifacts were discarded"
        )
        result_path.unlink(missing_ok=True)
        shutil.rmtree(artifact_dir, ignore_errors=True)
    elif outcome.returncode == 0:
        if result_path.is_symlink() or not result_path.is_file():
            result_error = "command completed without a regular result file"
        else:
            result_sha = _sha256_file(result_path)
            try:
                supplied = parse_certification_input(
                    _load_unique_json(result_path)
                )
                if supplied.spec_hash != spec.declaration_hash():
                    raise EvidenceProducerError(
                        "command result spec_hash differs from current declaration"
                    )
            except (CertificationInvariantError, EvidenceProducerError) as exc:
                result_error = str(exc)
    else:
        result_error = outcome.launch_error or (
            "command timed out" if outcome.timed_out else "command returned non-zero"
        )

    try:
        if supplied is None:
            shutil.rmtree(artifact_dir, ignore_errors=True)
            artifact_map = {"__run_id__": run_id}
            artifact_hashes = {}
        else:
            _validate_typed_stage_artifact(
                artifact_dir,
                spec=spec,
                stage=stage,
                supplied=supplied,
                started_at=outcome.started_at,
                completed_at=outcome.completed_at,
                argv=argv,
            )
            artifact_map, artifact_hashes = _collect_stage_artifacts(
                artifact_dir,
                output_root=output_root,
                run_id=run_id,
            )
            # Translation below is also the strict artifact-reference validator.
            seed = _blocked_input(
                spec,
                stage_products={
                    name: StageProduct(
                        state="blocked",
                        reason="stage pending",
                        receipt_path=(
                            f"provenance/receipts/{spec.source_id}/{name}/receipt.json"
                        ),
                        receipt_sha256="0" * 64,
                        supplied=None,
                        artifact_uri_map={"__run_id__": run_id},
                    )
                    for name in _STAGES
                },
                legacy_reference_count=1,
            )
            if stage == "local_correctness":
                _translate_local(seed, supplied, artifact_map)
            elif stage == "load":
                _translate_load(seed, supplied, artifact_map)
            elif stage == "fault_recovery":
                _translate_recovery(seed, supplied, artifact_map)
            else:
                _translate_canary(seed, supplied, artifact_map)
    except EvidenceProducerError as exc:
        supplied = None
        result_error = str(exc)
        shutil.rmtree(artifact_dir, ignore_errors=True)
        artifact_map = {"__run_id__": run_id}
        artifact_hashes = {}
    finally:
        result_path.unlink(missing_ok=True)

    state = (
        _stage_state(stage, supplied)
        if supplied is not None and result_error is None
        else "failed"
    )
    reason = (
        "command produced strict artifact-backed evidence"
        if state == "passed"
        else result_error
        or f"command result state is {state}"
    )
    relative = f"provenance/receipts/{spec.source_id}/{stage}/receipt.json"
    receipt_value = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": run_id,
        "commit_sha": commit_sha,
        "source_id": spec.source_id,
        "stage": stage,
        "state": state,
        "reason_code": (
            "command_evidence_accepted"
            if state == "passed"
            else "command_evidence_rejected"
        ),
        "reason": reason,
        "binding_path": binding.relative_path,
        "binding_sha256": binding.sha256,
        "command": list(argv),
        "required_environment": presence,
        "credential_environment_names": list(command.credential_env),
        "started_at": outcome.started_at.isoformat(),
        "completed_at": outcome.completed_at.isoformat(),
        "returncode": outcome.returncode,
        "timed_out": outcome.timed_out,
        "stdout_sha256": (
            None if leaked_output else _sha256_bytes(outcome.stdout)
        ),
        "stdout_bytes": 0 if leaked_output else len(outcome.stdout),
        "stderr_sha256": (
            None if leaked_output else _sha256_bytes(outcome.stderr)
        ),
        "stderr_bytes": 0 if leaked_output else len(outcome.stderr),
        "result_sha256": (
            result_sha
            if not leaked_output and supplied is not None
            else None
        ),
        "artifact_sha256": artifact_hashes,
    }
    receipt_path, receipt_sha = _write_receipt(
        provenance_root,
        relative_path=relative,
        value=receipt_value,
    )
    return StageProduct(
        state=state,
        reason=reason,
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha,
        supplied=supplied,
        artifact_uri_map=artifact_map,
    )


def _architecture_receipt(
    *,
    output_root: Path,
    provenance_root: Path,
    repo_root: Path,
    run_id: str,
    commit_sha: str,
    executor: CommandExecutor,
    ambient_env: Mapping[str, str],
) -> tuple[int, str, str]:
    argv = (
        sys.executable,
        str(repo_root / "scripts/check_source_architecture_ratchet.py"),
        "--no-baseline",
    )
    env = {
        name: value
        for name in _SAFE_ENV_NAMES
        if (value := ambient_env.get(name))
    }
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    outcome = executor(argv, repo_root, env, 300)
    if outcome.returncode == 0:
        count = 0
    elif outcome.returncode == 1:
        rendered = outcome.stderr.decode("utf-8", errors="replace")
        match = _ARCHITECTURE_COUNT_RE.search(rendered)
        count = int(match.group(1)) if match else 1
    else:
        count = 1
    relative = "provenance/receipts/architecture-ratchet.json"
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": run_id,
        "commit_sha": commit_sha,
        "source_id": None,
        "stage": "architecture_ratchet",
        "state": "passed" if outcome.returncode == 0 else "failed",
        "reason_code": (
            "strict_architecture_clean"
            if outcome.returncode == 0
            else "strict_architecture_not_clean"
        ),
        "reason": (
            "strict/no-baseline source architecture ratchet passed"
            if outcome.returncode == 0
            else "strict/no-baseline source architecture ratchet failed"
        ),
        "binding_path": "scripts/check_source_architecture_ratchet.py",
        "binding_sha256": _sha256_file(
            repo_root / "scripts/check_source_architecture_ratchet.py"
        ),
        "command": list(argv),
        "required_environment": {},
        "credential_environment_names": [],
        "started_at": outcome.started_at.isoformat(),
        "completed_at": outcome.completed_at.isoformat(),
        "returncode": outcome.returncode,
        "timed_out": outcome.timed_out,
        "stdout_sha256": _sha256_bytes(outcome.stdout),
        "stdout_bytes": len(outcome.stdout),
        "stderr_sha256": _sha256_bytes(outcome.stderr),
        "stderr_bytes": len(outcome.stderr),
        "result_sha256": None,
        "artifact_sha256": {},
        "legacy_reference_count": count,
    }
    path, digest = _write_receipt(
        provenance_root,
        relative_path=relative,
        value=receipt,
    )
    return count, path, digest


def _tree_hashes(root: Path, *, exclude: frozenset[str] = frozenset()) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise EvidenceProducerError(f"evidence tree contains symlink: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise EvidenceProducerError(
                f"evidence tree contains non-regular entry: {path}"
            )
        relative = path.relative_to(root).as_posix()
        if relative in exclude:
            continue
        hashes[relative] = _sha256_file(path)
    return hashes


def produce_evidence(
    *,
    repo_root: Path,
    binding_dir: Path,
    output_dir: Path,
    commit_sha: str | None = None,
    run_id: str | None = None,
    ambient_env: Mapping[str, str] | None = None,
    executor: CommandExecutor | None = None,
    repository_identity: RepositoryIdentity | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> dict[str, Any]:
    """Execute declared bindings and atomically publish a full or shard bundle."""

    repo_root = repo_root.resolve()
    output_dir = output_dir.resolve()
    try:
        output_dir.relative_to(repo_root)
    except ValueError:
        pass
    else:
        raise EvidenceProducerError(
            "evidence output directory must be outside the repository"
        )
    if output_dir.exists():
        raise EvidenceProducerError(
            f"evidence output directory already exists: {output_dir}"
        )
    identity = repository_identity or inspect_repository(
        repo_root,
        expected_commit_sha=commit_sha,
    )
    if _COMMIT_RE.fullmatch(identity.commit_sha) is None:
        raise EvidenceProducerError("invalid repository commit identity")
    now = datetime.now(timezone.utc)
    resolved_run_id = run_id or (
        f"{identity.commit_sha[:12]}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    )
    if not resolved_run_id or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for character in resolved_run_id
    ):
        raise EvidenceProducerError("run_id contains unsupported characters")
    if (shard_index is None) != (shard_count is None):
        raise EvidenceProducerError(
            "shard_index and shard_count must be provided together"
        )
    selected_source_ids = (
        tuple(SOURCE_CERTIFICATION_CATALOG)
        if shard_index is None or shard_count is None
        else deterministic_source_shard(shard_index, shard_count)
    )
    spec_hashes, catalog_sha256 = certification_catalog_identity()
    bindings, binding_errors = _load_bindings(
        binding_dir.resolve(),
        repo_root=repo_root,
    )
    command_executor = executor or _default_executor
    environment = dict(ambient_env if ambient_env is not None else os.environ)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            dir=output_dir.parent,
            prefix=f".{output_dir.name}.",
        )
    )
    inputs_root = temporary / "inputs"
    provenance_root = temporary / "provenance"
    inputs_root.mkdir()
    provenance_root.mkdir()
    source_entries: list[dict[str, Any]] = []
    try:
        legacy_count, architecture_path, architecture_sha = _architecture_receipt(
            output_root=temporary,
            provenance_root=provenance_root,
            repo_root=repo_root,
            run_id=resolved_run_id,
            commit_sha=identity.commit_sha,
            executor=command_executor,
            ambient_env=environment,
        )
        for source_id in selected_source_ids:
            spec = SOURCE_CERTIFICATION_CATALOG[source_id]
            binding = bindings.get(source_id)
            products: dict[str, StageProduct] = {}
            for stage in _STAGES:
                if not identity.clean:
                    products[stage] = _missing_stage_product(
                        output_root=temporary,
                        provenance_root=provenance_root,
                        run_id=resolved_run_id,
                        commit_sha=identity.commit_sha,
                        source_id=source_id,
                        stage=stage,
                        reason_code="repository_not_exact_clean_commit",
                        reason=(
                            "repository must be an unchanged clean checkout of "
                            "the target commit before evidence commands run"
                        ),
                        binding=binding,
                    )
                elif source_id in binding_errors:
                    products[stage] = _missing_stage_product(
                        output_root=temporary,
                        provenance_root=provenance_root,
                        run_id=resolved_run_id,
                        commit_sha=identity.commit_sha,
                        source_id=source_id,
                        stage=stage,
                        reason_code="invalid_executable_binding",
                        reason=binding_errors[source_id],
                        binding=None,
                    )
                elif binding is None:
                    products[stage] = _missing_stage_product(
                        output_root=temporary,
                        provenance_root=provenance_root,
                        run_id=resolved_run_id,
                        commit_sha=identity.commit_sha,
                        source_id=source_id,
                        stage=stage,
                        reason_code="source_execution_binding_absent",
                        reason=(
                            f"source execution binding is absent: "
                            f"{binding_dir / f'{source_id}.json'}"
                        ),
                        binding=None,
                    )
                else:
                    products[stage] = _execute_stage(
                        output_root=temporary,
                        provenance_root=provenance_root,
                        repo_root=repo_root,
                        run_id=resolved_run_id,
                        commit_sha=identity.commit_sha,
                        spec=spec,
                        stage=stage,
                        binding=binding,
                        ambient_env=environment,
                        executor=command_executor,
                    )
            supplied = _blocked_input(
                spec,
                stage_products=products,
                legacy_reference_count=legacy_count,
            )
            for stage in _STAGES:
                supplied = _apply_stage(
                    supplied,
                    stage=stage,
                    product=products[stage],
                )
            input_path = inputs_root / f"{source_id}.json"
            write_certification_input(input_path, supplied)
            decision = evaluate_certification(spec, supplied)
            source_entries.append(
                {
                    "source_id": source_id,
                    "spec_hash": spec.declaration_hash(),
                    "binding_path": (
                        binding.relative_path if binding is not None else None
                    ),
                    "binding_sha256": binding.sha256 if binding is not None else None,
                    "input_path": f"inputs/{source_id}.json",
                    "input_sha256": _sha256_file(input_path),
                    "stage_receipts": {
                        stage: {
                            "path": products[stage].receipt_path,
                            "sha256": products[stage].receipt_sha256,
                            "state": products[stage].state,
                        }
                        for stage in _STAGES
                    },
                    "decision_state": decision.state,
                    "decision_failures": list(decision.failures),
                }
            )

        final_identity = inspect_repository(
            repo_root,
            expected_commit_sha=identity.commit_sha,
        )
        files = _tree_hashes(provenance_root)
        producer_state = (
            "passed"
            if identity.clean
            and final_identity.clean
            and legacy_count == 0
            and all(entry["decision_state"] == "passed" for entry in source_entries)
            else "blocked"
        )
        manifest: dict[str, Any] = {
            "schema_version": (
                PRODUCER_SCHEMA_VERSION
                if shard_index is None
                else PRODUCER_SHARD_SCHEMA_VERSION
            ),
            "run_id": resolved_run_id,
            "commit_sha": identity.commit_sha,
            "started_at": now.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "state": producer_state,
            "required_sources": len(selected_source_ids),
            "source_order": list(selected_source_ids),
            "repository": {
                "initial_head_sha": identity.head_sha,
                "initial_clean": identity.clean,
                "initial_status_sha256": identity.status_sha256,
                "initial_status_entry_count": identity.status_entry_count,
                "final_head_sha": final_identity.head_sha,
                "final_clean": final_identity.clean,
                "final_status_sha256": final_identity.status_sha256,
                "final_status_entry_count": final_identity.status_entry_count,
            },
            "architecture": {
                "legacy_reference_count": legacy_count,
                "receipt_path": architecture_path,
                "receipt_sha256": architecture_sha,
            },
            "sources": source_entries,
            "provenance_files": files,
        }
        if shard_index is not None and shard_count is not None:
            manifest["shard"] = {
                "index": shard_index,
                "count": shard_count,
                "catalog_source_order": list(SOURCE_CERTIFICATION_CATALOG),
                "catalog_spec_hashes": spec_hashes,
                "catalog_sha256": catalog_sha256,
            }
        _atomic_write_json(
            provenance_root / "producer-manifest.json",
            manifest,
        )
        temporary.replace(output_dir)
        return manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _safe_relative_path(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise EvidenceProducerError(f"{field} must be a non-empty relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise EvidenceProducerError(f"{field} is not a canonical relative path")
    return value


def _verify_artifact_uri(
    uri: str,
    *,
    run_id: str,
    provenance_root: Path,
    declared_files: Mapping[str, str],
) -> None:
    prefix = f"{_ARTIFACT_PREFIX}{run_id}/provenance/"
    if not uri.startswith(prefix) or "#sha256=" not in uri:
        raise EvidenceProducerError(
            f"artifact URI is not bound to producer run {run_id!r}: {uri!r}"
        )
    relative_with_fragment = uri[len(prefix) :]
    relative, digest = relative_with_fragment.rsplit("#sha256=", 1)
    provenance_relative = _safe_relative_path(relative, field="artifact URI path")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise EvidenceProducerError(f"artifact URI has invalid SHA-256: {uri!r}")
    declared = declared_files.get(provenance_relative)
    if declared != digest:
        raise EvidenceProducerError(
            f"artifact URI checksum is not declared by producer: {uri!r}"
        )
    path = provenance_root / provenance_relative
    if path.is_symlink() or not path.is_file() or _sha256_file(path) != digest:
        raise EvidenceProducerError(f"artifact URI target is missing or changed: {uri!r}")


def _input_artifact_uris(value: CertificationInput) -> tuple[str, ...]:
    uris = []
    if value.local_correctness_artifact:
        uris.append(value.local_correctness_artifact)
    uris.extend(result.artifact_uri for result in value.scenario_results)
    for suites in (
        value.provider_safe_suites,
        value.fyralis_ceiling_suites,
        value.fault_recovery_suites,
    ):
        uris.extend(
            suite.artifact_uri for suite in suites if suite.artifact_uri is not None
        )
    if value.canary.artifact_uri:
        uris.append(value.canary.artifact_uri)
    uris.extend(
        result.artifact_uri for result in value.canary.operation_results
    )
    return tuple(uris)


def _verify_typed_stage_artifact(
    *,
    provenance_root: Path,
    receipt_raw: Mapping[str, Any],
    source_id: str,
    stage: str,
    spec: SourceCertificationSpec,
    supplied: CertificationInput,
) -> None:
    artifact_hashes = receipt_raw.get("artifact_sha256")
    if not isinstance(artifact_hashes, Mapping):
        raise EvidenceProducerError(
            f"{source_id}.{stage} artifact checksums are invalid"
        )
    expected_path = (
        f"provenance/receipts/{source_id}/{stage}/artifacts/stage.json"
    )
    matching = [
        (path, digest)
        for path, digest in artifact_hashes.items()
        if path == expected_path
    ]
    if len(matching) != 1:
        raise EvidenceProducerError(
            f"{source_id}.{stage} has no unique typed stage.json artifact"
        )
    path, digest = matching[0]
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise EvidenceProducerError(
            f"{source_id}.{stage} stage artifact checksum is invalid"
        )
    artifact_path = provenance_root / path[len("provenance/") :]
    if (
        artifact_path.is_symlink()
        or not artifact_path.is_file()
        or _sha256_file(artifact_path) != digest
    ):
        raise EvidenceProducerError(
            f"{source_id}.{stage} stage artifact is missing or changed"
        )
    command = receipt_raw.get("command")
    if not isinstance(command, list) or not all(
        isinstance(item, str) for item in command
    ):
        raise EvidenceProducerError(
            f"{source_id}.{stage} receipt command is invalid"
        )
    try:
        validate_stage_artifact(
            _load_unique_json(artifact_path),
            spec=spec,
            stage=stage,  # type: ignore[arg-type]
            supplied=supplied,
            started_at=_receipt_timestamp(
                receipt_raw.get("started_at"),
                field=f"{source_id}.{stage}.started_at",
            ),
            completed_at=_receipt_timestamp(
                receipt_raw.get("completed_at"),
                field=f"{source_id}.{stage}.completed_at",
            ),
            expected_plan_sha256=_expected_plan_sha256(command),
        )
    except StageArtifactError as exc:
        raise EvidenceProducerError(
            f"{source_id}.{stage} stage artifact is invalid: {exc}"
        ) from exc


def verify_evidence_bundle(
    *,
    repo_root: Path,
    input_dir: Path,
    provenance_dir: Path,
    expected_commit_sha: str,
    require_complete: bool = True,
) -> dict[str, Any]:
    """Verify producer provenance before the release evaluator reads inputs."""

    if _COMMIT_RE.fullmatch(expected_commit_sha) is None:
        raise EvidenceProducerError("expected commit SHA is invalid")
    for path, label in (
        (input_dir, "input directory"),
        (provenance_dir, "provenance directory"),
    ):
        if path.is_symlink() or not path.is_dir():
            raise EvidenceProducerError(f"{label} must be a regular directory")
    manifest_path = provenance_dir / "producer-manifest.json"
    raw = _load_unique_json(manifest_path)
    if not isinstance(raw, Mapping):
        raise EvidenceProducerError("producer manifest must be an object")
    expected_keys = frozenset(
        {
            "schema_version",
            "run_id",
            "commit_sha",
            "started_at",
            "completed_at",
            "state",
            "required_sources",
            "source_order",
            "repository",
            "architecture",
            "sources",
            "provenance_files",
        }
    )
    _exact_keys(raw, expected_keys, field="producer manifest")
    if raw["schema_version"] != PRODUCER_SCHEMA_VERSION:
        raise EvidenceProducerError("producer manifest schema version is unsupported")
    if raw["commit_sha"] != expected_commit_sha:
        raise EvidenceProducerError("producer manifest commit differs from target")
    if raw["state"] not in {"passed", "blocked"}:
        raise EvidenceProducerError("producer manifest state is invalid")
    if require_complete and raw["state"] != "passed":
        raise EvidenceProducerError(
            f"producer manifest state must equal passed, got {raw['state']!r}"
        )
    source_order = list(SOURCE_CERTIFICATION_CATALOG)
    if raw["source_order"] != source_order:
        raise EvidenceProducerError("producer source order differs from catalog")
    if raw["required_sources"] != len(source_order):
        raise EvidenceProducerError("producer required source count is invalid")
    declared_files = raw["provenance_files"]
    if not isinstance(declared_files, Mapping) or not all(
        isinstance(path, str)
        and isinstance(digest, str)
        and _SHA256_RE.fullmatch(digest) is not None
        for path, digest in declared_files.items()
    ):
        raise EvidenceProducerError("provenance_files must be a checksum mapping")
    for path in declared_files:
        _safe_relative_path(path, field="provenance_files path")
    actual_files = _tree_hashes(
        provenance_dir,
        exclude=frozenset({"producer-manifest.json"}),
    )
    if dict(declared_files) != actual_files:
        raise EvidenceProducerError("provenance file inventory or checksum differs")
    repository = raw["repository"]
    if not isinstance(repository, Mapping):
        raise EvidenceProducerError("repository provenance must be an object")
    _exact_keys(
        repository,
        frozenset(
            {
                "initial_head_sha",
                "initial_clean",
                "initial_status_sha256",
                "initial_status_entry_count",
                "final_head_sha",
                "final_clean",
                "final_status_sha256",
                "final_status_entry_count",
            }
        ),
        field="repository provenance",
    )
    if require_complete and (
        repository.get("initial_head_sha") != expected_commit_sha
        or repository.get("final_head_sha") != expected_commit_sha
        or repository.get("initial_clean") is not True
        or repository.get("final_clean") is not True
    ):
        raise EvidenceProducerError(
            "complete evidence requires an unchanged clean target worktree"
        )
    architecture = raw["architecture"]
    if not isinstance(architecture, Mapping):
        raise EvidenceProducerError("architecture provenance must be an object")
    _exact_keys(
        architecture,
        frozenset(
            {
                "legacy_reference_count",
                "receipt_path",
                "receipt_sha256",
            }
        ),
        field="architecture provenance",
    )
    legacy_count = architecture.get("legacy_reference_count")
    if (
        isinstance(legacy_count, bool)
        or not isinstance(legacy_count, int)
        or legacy_count < 0
    ):
        raise EvidenceProducerError("architecture legacy reference count is invalid")
    if require_complete and legacy_count != 0:
        raise EvidenceProducerError("complete evidence requires zero legacy references")
    architecture_path = _safe_relative_path(
        architecture["receipt_path"],
        field="architecture receipt path",
    )
    if not architecture_path.startswith("provenance/"):
        raise EvidenceProducerError("architecture receipt must live in provenance")
    architecture_relative = architecture_path[len("provenance/") :]
    if declared_files.get(architecture_relative) != architecture["receipt_sha256"]:
        raise EvidenceProducerError("architecture receipt checksum differs")
    architecture_receipt = _load_unique_json(
        provenance_dir / architecture_relative
    )
    if not isinstance(architecture_receipt, Mapping):
        raise EvidenceProducerError("architecture receipt must be an object")
    _exact_keys(
        architecture_receipt,
        _STAGE_RECEIPT_FIELDS | frozenset({"legacy_reference_count"}),
        field="architecture receipt",
    )
    if (
        architecture_receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or architecture_receipt.get("run_id") != raw["run_id"]
        or architecture_receipt.get("commit_sha") != expected_commit_sha
        or architecture_receipt.get("source_id") is not None
        or architecture_receipt.get("stage") != "architecture_ratchet"
        or architecture_receipt.get("legacy_reference_count") != legacy_count
    ):
        raise EvidenceProducerError("architecture receipt identity differs")
    if require_complete and (
        architecture_receipt.get("state") != "passed"
        or architecture_receipt.get("returncode") != 0
    ):
        raise EvidenceProducerError("complete evidence requires a passed architecture receipt")
    sources = raw["sources"]
    if not isinstance(sources, list) or [
        entry.get("source_id") if isinstance(entry, Mapping) else None
        for entry in sources
    ] != source_order:
        raise EvidenceProducerError(
            "producer sources must cover the canonical catalog exactly once"
        )
    expected_input_names = {f"{source_id}.json" for source_id in source_order}
    actual_input_names = set()
    for path in input_dir.iterdir():
        if path.is_symlink() or not path.is_file():
            raise EvidenceProducerError(
                f"input bundle contains non-regular entry: {path.name}"
            )
        actual_input_names.add(path.name)
    if actual_input_names != expected_input_names:
        raise EvidenceProducerError("input bundle membership differs from catalog")

    for entry, (source_id, spec) in zip(
        sources,
        SOURCE_CERTIFICATION_CATALOG.items(),
        strict=True,
    ):
        assert isinstance(entry, Mapping)
        _exact_keys(
            entry,
            frozenset(
                {
                    "source_id",
                    "spec_hash",
                    "binding_path",
                    "binding_sha256",
                    "input_path",
                    "input_sha256",
                    "stage_receipts",
                    "decision_state",
                    "decision_failures",
                }
            ),
            field=f"{source_id} producer entry",
        )
        if entry.get("spec_hash") != spec.declaration_hash():
            raise EvidenceProducerError(f"{source_id} producer spec hash is stale")
        input_path = input_dir / f"{source_id}.json"
        if entry.get("input_path") != f"inputs/{source_id}.json":
            raise EvidenceProducerError(f"{source_id} input path is invalid")
        if entry.get("input_sha256") != _sha256_file(input_path):
            raise EvidenceProducerError(f"{source_id} input checksum differs")
        supplied = load_certification_input(input_path)
        if supplied.spec_hash != spec.declaration_hash():
            raise EvidenceProducerError(f"{source_id} input spec hash is stale")
        if supplied.legacy_reference_count != legacy_count:
            raise EvidenceProducerError(
                f"{source_id} input legacy count differs from architecture receipt"
            )
        stage_receipts = entry.get("stage_receipts")
        if not isinstance(stage_receipts, Mapping) or set(stage_receipts) != set(_STAGES):
            raise EvidenceProducerError(f"{source_id} stage receipts are incomplete")
        for stage in _STAGES:
            receipt = stage_receipts[stage]
            if not isinstance(receipt, Mapping):
                raise EvidenceProducerError(
                    f"{source_id}.{stage} receipt declaration is invalid"
                )
            _exact_keys(
                receipt,
                frozenset({"path", "sha256", "state"}),
                field=f"{source_id}.{stage} receipt declaration",
            )
            receipt_path_value = _safe_relative_path(
                receipt.get("path"),
                field=f"{source_id}.{stage}.receipt.path",
            )
            if not receipt_path_value.startswith("provenance/"):
                raise EvidenceProducerError(
                    f"{source_id}.{stage} receipt must live in provenance"
                )
            relative = receipt_path_value[len("provenance/") :]
            if declared_files.get(relative) != receipt.get("sha256"):
                raise EvidenceProducerError(
                    f"{source_id}.{stage} receipt checksum differs"
                )
            receipt_raw = _load_unique_json(provenance_dir / relative)
            if not isinstance(receipt_raw, Mapping):
                raise EvidenceProducerError(
                    f"{source_id}.{stage} receipt must be an object"
                )
            _exact_keys(
                receipt_raw,
                _STAGE_RECEIPT_FIELDS,
                field=f"{source_id}.{stage} receipt",
            )
            if (
                receipt_raw.get("schema_version") != RECEIPT_SCHEMA_VERSION
                or receipt_raw.get("run_id") != raw["run_id"]
                or receipt_raw.get("commit_sha") != expected_commit_sha
                or receipt_raw.get("source_id") != source_id
                or receipt_raw.get("stage") != stage
            ):
                raise EvidenceProducerError(
                    f"{source_id}.{stage} receipt identity differs"
                )
            if require_complete and receipt_raw.get("state") != "passed":
                raise EvidenceProducerError(
                    f"{source_id}.{stage} receipt state is not passed"
                )
            if require_complete:
                if (
                    receipt_raw.get("returncode") != 0
                    or receipt_raw.get("timed_out") is not False
                    or not isinstance(receipt_raw.get("command"), list)
                    or not receipt_raw["command"]
                    or not isinstance(receipt_raw.get("result_sha256"), str)
                    or _SHA256_RE.fullmatch(receipt_raw["result_sha256"]) is None
                ):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} has no successful command result"
                    )
                required_environment = receipt_raw.get("required_environment")
                if not isinstance(required_environment, Mapping) or not all(
                    isinstance(name, str) and present is True
                    for name, present in required_environment.items()
                ):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} required environment was incomplete"
                    )
                credential_names = receipt_raw.get(
                    "credential_environment_names"
                )
                if not isinstance(credential_names, list) or not all(
                    isinstance(name, str) for name in credential_names
                ):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} credential receipt is invalid"
                    )
                if stage == "canary":
                    prefix = spec.canary.credential_env_prefix
                    accepted_prefix = f"{prefix}_"
                    if not credential_names or not all(
                        (
                            name == prefix or name.startswith(accepted_prefix)
                        )
                        and required_environment.get(name) is True
                        for name in credential_names
                    ):
                        raise EvidenceProducerError(
                            f"{source_id} canary has no source-scoped credential proof"
                        )
                elif credential_names:
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} received canary credentials"
                    )
                if (
                    receipt_raw.get("binding_path") != entry.get("binding_path")
                    or receipt_raw.get("binding_sha256")
                    != entry.get("binding_sha256")
                ):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} binding receipt differs"
                    )
                artifact_hashes = receipt_raw.get("artifact_sha256")
                if not isinstance(artifact_hashes, Mapping):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} artifact checksums are invalid"
                    )
                for artifact_path, artifact_sha in artifact_hashes.items():
                    if (
                        not isinstance(artifact_path, str)
                        or not artifact_path.startswith("provenance/")
                        or not isinstance(artifact_sha, str)
                        or declared_files.get(
                            artifact_path[len("provenance/") :]
                        )
                        != artifact_sha
                    ):
                        raise EvidenceProducerError(
                            f"{source_id}.{stage} artifact receipt differs"
                        )
            if receipt_raw.get("result_sha256") is not None:
                _verify_typed_stage_artifact(
                    provenance_root=provenance_dir,
                    receipt_raw=receipt_raw,
                    source_id=source_id,
                    stage=stage,
                    spec=spec,
                    supplied=supplied,
                )
        binding_path = entry.get("binding_path")
        binding_sha = entry.get("binding_sha256")
        if require_complete:
            relative_binding = _safe_relative_path(
                binding_path,
                field=f"{source_id}.binding_path",
            )
            binding_file = repo_root / relative_binding
            if (
                binding_file.is_symlink()
                or not binding_file.is_file()
                or _sha256_file(binding_file) != binding_sha
            ):
                raise EvidenceProducerError(
                    f"{source_id} execution binding is missing or changed"
                )
        for uri in _input_artifact_uris(supplied):
            _verify_artifact_uri(
                uri,
                run_id=raw["run_id"],
                provenance_root=provenance_dir,
                declared_files=declared_files,
            )
        decision = evaluate_certification(spec, supplied)
        if entry.get("decision_state") != decision.state:
            raise EvidenceProducerError(
                f"{source_id} producer decision state differs on replay"
            )
        if require_complete and decision.state != "passed":
            raise EvidenceProducerError(
                f"{source_id} certification replay is {decision.state}"
            )
    if require_complete:
        current_identity = inspect_repository(
            repo_root,
            expected_commit_sha=expected_commit_sha,
        )
        if not current_identity.clean:
            raise EvidenceProducerError(
                "evidence consumer repository is not the exact clean target commit"
            )
    return {
        "state": raw["state"],
        "run_id": raw["run_id"],
        "commit_sha": raw["commit_sha"],
        "verified_sources": len(source_order),
        "provenance_files": len(declared_files),
    }


def merge_evidence_shards(
    *,
    repo_root: Path,
    shard_dirs: Sequence[Path],
    output_dir: Path,
    expected_commit_sha: str | None = None,
) -> dict[str, Any]:
    """Fail closed while merging deterministic shards into a v1 full bundle."""

    repo_root = repo_root.resolve()
    output_dir = output_dir.resolve()
    try:
        output_dir.relative_to(repo_root)
    except ValueError:
        pass
    else:
        raise EvidenceProducerError(
            "evidence output directory must be outside the repository"
        )
    if output_dir.exists():
        raise EvidenceProducerError(
            f"evidence output directory already exists: {output_dir}"
        )
    if not shard_dirs:
        raise EvidenceProducerError("at least one evidence shard is required")

    canonical_order = list(SOURCE_CERTIFICATION_CATALOG)
    spec_hashes, catalog_sha256 = certification_catalog_identity()
    expected_manifest_keys = frozenset(
        {
            "schema_version",
            "run_id",
            "commit_sha",
            "started_at",
            "completed_at",
            "state",
            "required_sources",
            "source_order",
            "repository",
            "architecture",
            "sources",
            "provenance_files",
            "shard",
        }
    )
    expected_repository_keys = frozenset(
        {
            "initial_head_sha",
            "initial_clean",
            "initial_status_sha256",
            "initial_status_entry_count",
            "final_head_sha",
            "final_clean",
            "final_status_sha256",
            "final_status_entry_count",
        }
    )
    expected_shard_keys = frozenset(
        {
            "index",
            "count",
            "catalog_source_order",
            "catalog_spec_hashes",
            "catalog_sha256",
        }
    )
    manifests_by_index: dict[int, tuple[Path, Mapping[str, Any]]] = {}
    common_commit = expected_commit_sha
    common_run_id: str | None = None
    common_count: int | None = None
    common_repository: Mapping[str, Any] | None = None
    common_legacy_count: int | None = None

    for unresolved_shard_dir in shard_dirs:
        if unresolved_shard_dir.is_symlink():
            raise EvidenceProducerError(
                f"evidence shard must not be a symlink: {unresolved_shard_dir}"
            )
        shard_dir = unresolved_shard_dir.resolve()
        inputs_dir = shard_dir / "inputs"
        provenance_dir = shard_dir / "provenance"
        if shard_dir.is_symlink() or not shard_dir.is_dir():
            raise EvidenceProducerError(
                f"evidence shard must be a regular directory: {shard_dir}"
            )
        for path, label in (
            (inputs_dir, "shard input directory"),
            (provenance_dir, "shard provenance directory"),
        ):
            if path.is_symlink() or not path.is_dir():
                raise EvidenceProducerError(f"{label} must be a regular directory")

        raw = _load_unique_json(provenance_dir / "producer-manifest.json")
        if not isinstance(raw, Mapping):
            raise EvidenceProducerError("shard producer manifest must be an object")
        _exact_keys(raw, expected_manifest_keys, field="shard producer manifest")
        if raw["schema_version"] != PRODUCER_SHARD_SCHEMA_VERSION:
            raise EvidenceProducerError("producer shard schema version is unsupported")
        commit_sha = raw["commit_sha"]
        if not isinstance(commit_sha, str) or _COMMIT_RE.fullmatch(commit_sha) is None:
            raise EvidenceProducerError("producer shard commit is invalid")
        if common_commit is None:
            common_commit = commit_sha
        if commit_sha != common_commit:
            raise EvidenceProducerError("producer shard commits differ")
        run_id = raw["run_id"]
        if not isinstance(run_id, str) or not run_id:
            raise EvidenceProducerError("producer shard run_id is invalid")
        if common_run_id is None:
            common_run_id = run_id
        if run_id != common_run_id:
            raise EvidenceProducerError("producer shard run IDs differ")
        if raw["state"] not in {"passed", "blocked"}:
            raise EvidenceProducerError("producer shard state is invalid")

        shard = raw["shard"]
        if not isinstance(shard, Mapping):
            raise EvidenceProducerError("producer shard identity must be an object")
        _exact_keys(shard, expected_shard_keys, field="producer shard identity")
        index = shard["index"]
        count = shard["count"]
        if (
            isinstance(index, bool)
            or isinstance(count, bool)
            or not isinstance(index, int)
            or not isinstance(count, int)
        ):
            raise EvidenceProducerError("producer shard index/count is invalid")
        expected_sources = list(deterministic_source_shard(index, count))
        if common_count is None:
            common_count = count
        if count != common_count:
            raise EvidenceProducerError("producer shard counts differ")
        if index in manifests_by_index:
            raise EvidenceProducerError(f"duplicate producer shard index: {index}")
        if (
            shard["catalog_source_order"] != canonical_order
            or shard["catalog_spec_hashes"] != spec_hashes
            or shard["catalog_sha256"] != catalog_sha256
        ):
            raise EvidenceProducerError(
                "producer shard catalog/spec identity differs from checkout"
            )
        if (
            raw["source_order"] != expected_sources
            or raw["required_sources"] != len(expected_sources)
        ):
            raise EvidenceProducerError(
                f"producer shard {index} membership is not deterministic"
            )

        repository = raw["repository"]
        if not isinstance(repository, Mapping):
            raise EvidenceProducerError("shard repository provenance must be an object")
        _exact_keys(
            repository,
            expected_repository_keys,
            field="shard repository provenance",
        )
        if (
            repository["initial_head_sha"] != common_commit
            or repository["final_head_sha"] != common_commit
            or repository["initial_clean"] is not True
            or repository["final_clean"] is not True
            or repository["initial_status_entry_count"] != 0
            or repository["final_status_entry_count"] != 0
            or repository["initial_status_sha256"] != _sha256_bytes(b"")
            or repository["final_status_sha256"] != _sha256_bytes(b"")
        ):
            raise EvidenceProducerError(
                "every shard requires an unchanged clean target worktree"
            )
        if common_repository is None:
            common_repository = repository
        if dict(repository) != dict(common_repository):
            raise EvidenceProducerError("producer shard repository identities differ")

        declared_files = raw["provenance_files"]
        if not isinstance(declared_files, Mapping) or not all(
            isinstance(path, str)
            and isinstance(digest, str)
            and _SHA256_RE.fullmatch(digest) is not None
            for path, digest in declared_files.items()
        ):
            raise EvidenceProducerError(
                "producer shard provenance_files must be a checksum mapping"
            )
        actual_files = _tree_hashes(
            provenance_dir,
            exclude=frozenset({"producer-manifest.json"}),
        )
        if dict(declared_files) != actual_files:
            raise EvidenceProducerError(
                "producer shard provenance inventory or checksum differs"
            )
        allowed_prefixes = tuple(
            f"receipts/{source_id}/" for source_id in expected_sources
        )
        for relative_path in declared_files:
            _safe_relative_path(
                relative_path,
                field="producer shard provenance path",
            )
            if (
                relative_path != "receipts/architecture-ratchet.json"
                and not relative_path.startswith(allowed_prefixes)
            ):
                raise EvidenceProducerError(
                    f"producer shard provenance is not source-isolated: "
                    f"{relative_path}"
                )

        architecture = raw["architecture"]
        if not isinstance(architecture, Mapping):
            raise EvidenceProducerError(
                "producer shard architecture provenance must be an object"
            )
        _exact_keys(
            architecture,
            frozenset(
                {
                    "legacy_reference_count",
                    "receipt_path",
                    "receipt_sha256",
                }
            ),
            field="producer shard architecture provenance",
        )
        legacy_count = architecture["legacy_reference_count"]
        if (
            isinstance(legacy_count, bool)
            or not isinstance(legacy_count, int)
            or legacy_count < 0
        ):
            raise EvidenceProducerError(
                "producer shard legacy reference count is invalid"
            )
        if common_legacy_count is None:
            common_legacy_count = legacy_count
        if legacy_count != common_legacy_count:
            raise EvidenceProducerError(
                "producer shard architecture results differ"
            )
        if architecture["receipt_path"] != (
            "provenance/receipts/architecture-ratchet.json"
        ):
            raise EvidenceProducerError(
                "producer shard architecture receipt path is invalid"
            )
        architecture_sha = declared_files.get(
            "receipts/architecture-ratchet.json"
        )
        if architecture_sha != architecture["receipt_sha256"]:
            raise EvidenceProducerError(
                "producer shard architecture receipt checksum differs"
            )
        architecture_receipt = _load_unique_json(
            provenance_dir / "receipts/architecture-ratchet.json"
        )
        if not isinstance(architecture_receipt, Mapping):
            raise EvidenceProducerError(
                "producer shard architecture receipt must be an object"
            )
        _exact_keys(
            architecture_receipt,
            _STAGE_RECEIPT_FIELDS | frozenset({"legacy_reference_count"}),
            field="producer shard architecture receipt",
        )
        if (
            architecture_receipt["schema_version"] != RECEIPT_SCHEMA_VERSION
            or architecture_receipt["run_id"] != common_run_id
            or architecture_receipt["commit_sha"] != common_commit
            or architecture_receipt["source_id"] is not None
            or architecture_receipt["stage"] != "architecture_ratchet"
            or architecture_receipt["legacy_reference_count"] != legacy_count
        ):
            raise EvidenceProducerError(
                "producer shard architecture receipt identity differs"
            )
        if (
            architecture_receipt["state"]
            != ("passed" if legacy_count == 0 else "failed")
            or (
                legacy_count == 0
                and (
                    architecture_receipt["returncode"] != 0
                    or architecture_receipt["timed_out"] is not False
                )
            )
        ):
            raise EvidenceProducerError(
                "producer shard architecture receipt outcome differs"
            )

        expected_input_names = {
            f"{source_id}.json" for source_id in expected_sources
        }
        actual_input_names = set()
        for input_path in inputs_dir.iterdir():
            if input_path.is_symlink() or not input_path.is_file():
                raise EvidenceProducerError(
                    f"producer shard input contains non-regular entry: "
                    f"{input_path.name}"
                )
            actual_input_names.add(input_path.name)
        if actual_input_names != expected_input_names:
            raise EvidenceProducerError(
                f"producer shard {index} input membership differs"
            )
        sources = raw["sources"]
        if not isinstance(sources, list) or [
            entry.get("source_id") if isinstance(entry, Mapping) else None
            for entry in sources
        ] != expected_sources:
            raise EvidenceProducerError(
                f"producer shard {index} source entries differ"
            )
        for entry in sources:
            assert isinstance(entry, Mapping)
            source_id = entry["source_id"]
            if entry.get("spec_hash") != spec_hashes[source_id]:
                raise EvidenceProducerError(
                    f"{source_id} producer shard spec hash is stale"
                )
            input_path = inputs_dir / f"{source_id}.json"
            if (
                entry.get("input_path") != f"inputs/{source_id}.json"
                or entry.get("input_sha256") != _sha256_file(input_path)
            ):
                raise EvidenceProducerError(
                    f"{source_id} producer shard input identity differs"
                )
            supplied = load_certification_input(input_path)
            if supplied.spec_hash != spec_hashes[source_id]:
                raise EvidenceProducerError(
                    f"{source_id} producer shard input spec hash is stale"
                )
            source_provenance_prefix = (
                f"provenance/receipts/{source_id}/"
            )
            for artifact_uri in _input_artifact_uris(supplied):
                artifact_prefix = (
                    f"{_ARTIFACT_PREFIX}{common_run_id}/"
                    f"{source_provenance_prefix}"
                )
                if not artifact_uri.startswith(artifact_prefix):
                    raise EvidenceProducerError(
                        f"{source_id} artifact URI is not source-isolated"
                    )
            stage_receipts = entry.get("stage_receipts")
            if (
                not isinstance(stage_receipts, Mapping)
                or set(stage_receipts) != set(_STAGES)
            ):
                raise EvidenceProducerError(
                    f"{source_id} producer shard stage receipts are incomplete"
                )
            for stage in _STAGES:
                receipt = stage_receipts[stage]
                if (
                    not isinstance(receipt, Mapping)
                    or not isinstance(receipt.get("path"), str)
                ):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} receipt is not source-isolated"
                    )
                receipt_path = _safe_relative_path(
                    receipt["path"],
                    field=f"{source_id}.{stage} shard receipt path",
                )
                if not receipt_path.startswith(source_provenance_prefix):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} receipt is not source-isolated"
                    )
                receipt_relative = receipt_path[len("provenance/") :]
                if declared_files.get(receipt_relative) != receipt.get("sha256"):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} shard receipt checksum differs"
                    )
                receipt_raw = _load_unique_json(
                    provenance_dir / receipt_relative
                )
                if not isinstance(receipt_raw, Mapping):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} receipt must be an object"
                    )
                artifact_hashes = receipt_raw.get("artifact_sha256")
                if not isinstance(artifact_hashes, Mapping) or any(
                    not isinstance(artifact_path, str)
                    or not artifact_path.startswith(
                        source_provenance_prefix
                    )
                    for artifact_path in artifact_hashes
                ):
                    raise EvidenceProducerError(
                        f"{source_id}.{stage} artifacts are not source-isolated"
                    )
            decision = evaluate_certification(
                SOURCE_CERTIFICATION_CATALOG[source_id],
                supplied,
            )
            if entry.get("decision_state") != decision.state:
                raise EvidenceProducerError(
                    f"{source_id} producer shard decision differs on replay"
                )
        manifests_by_index[index] = (shard_dir, raw)

    assert common_count is not None
    assert common_commit is not None
    assert common_run_id is not None
    assert common_repository is not None
    assert common_legacy_count is not None
    expected_indexes = set(range(common_count))
    if set(manifests_by_index) != expected_indexes:
        missing = sorted(expected_indexes - set(manifests_by_index))
        extra = sorted(set(manifests_by_index) - expected_indexes)
        raise EvidenceProducerError(
            f"producer shard index coverage differs; missing={missing}, extra={extra}"
        )

    source_entries_by_id: dict[str, Mapping[str, Any]] = {}
    for index in range(common_count):
        _shard_dir, raw = manifests_by_index[index]
        for entry in raw["sources"]:
            source_id = entry["source_id"]
            if source_id in source_entries_by_id:
                raise EvidenceProducerError(
                    f"duplicate producer source across shards: {source_id}"
                )
            source_entries_by_id[source_id] = entry
    if set(source_entries_by_id) != set(canonical_order):
        raise EvidenceProducerError(
            "producer shards must cover the canonical catalog exactly once"
        )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            dir=output_dir.parent,
            prefix=f".{output_dir.name}.",
        )
    )
    inputs_root = temporary / "inputs"
    provenance_root = temporary / "provenance"
    inputs_root.mkdir()
    provenance_root.mkdir()
    try:
        architecture_source: Path | None = None
        architecture_metadata: Mapping[str, Any] | None = None
        for index in range(common_count):
            shard_dir, raw = manifests_by_index[index]
            for source_id in raw["source_order"]:
                shutil.copy2(
                    shard_dir / "inputs" / f"{source_id}.json",
                    inputs_root / f"{source_id}.json",
                )
                source_prefix = f"receipts/{source_id}/"
                for relative_path in raw["provenance_files"]:
                    if not relative_path.startswith(source_prefix):
                        continue
                    source_path = shard_dir / "provenance" / relative_path
                    target_path = provenance_root / relative_path
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    if target_path.exists():
                        raise EvidenceProducerError(
                            f"producer shard provenance collision: {relative_path}"
                        )
                    shutil.copy2(source_path, target_path)
            if architecture_source is None:
                architecture_source = (
                    shard_dir / "provenance/receipts/architecture-ratchet.json"
                )
                architecture_metadata = raw["architecture"]
        assert architecture_source is not None
        assert architecture_metadata is not None
        merged_architecture_path = (
            provenance_root / "receipts/architecture-ratchet.json"
        )
        merged_architecture_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(architecture_source, merged_architecture_path)

        source_entries = [
            dict(source_entries_by_id[source_id]) for source_id in canonical_order
        ]
        provenance_files = _tree_hashes(provenance_root)
        merged_manifest: dict[str, Any] = {
            "schema_version": PRODUCER_SCHEMA_VERSION,
            "run_id": common_run_id,
            "commit_sha": common_commit,
            "started_at": min(
                raw["started_at"] for _path, raw in manifests_by_index.values()
            ),
            "completed_at": max(
                raw["completed_at"] for _path, raw in manifests_by_index.values()
            ),
            "state": (
                "passed"
                if all(
                    raw["state"] == "passed"
                    for _path, raw in manifests_by_index.values()
                )
                and all(
                    entry["decision_state"] == "passed"
                    for entry in source_entries
                )
                else "blocked"
            ),
            "required_sources": len(canonical_order),
            "source_order": canonical_order,
            "repository": dict(common_repository),
            "architecture": {
                "legacy_reference_count": common_legacy_count,
                "receipt_path": (
                    "provenance/receipts/architecture-ratchet.json"
                ),
                "receipt_sha256": _sha256_file(merged_architecture_path),
            },
            "sources": source_entries,
            "provenance_files": provenance_files,
        }
        _atomic_write_json(
            provenance_root / "producer-manifest.json",
            merged_manifest,
        )
        verify_evidence_bundle(
            repo_root=repo_root,
            input_dir=inputs_root,
            provenance_dir=provenance_root,
            expected_commit_sha=common_commit,
            require_complete=False,
        )
        temporary.replace(output_dir)
        return merged_manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


__all__ = [
    "BINDING_SCHEMA_VERSION",
    "CommandExecutor",
    "CommandOutcome",
    "EvidenceProducerError",
    "ExecutionBinding",
    "PRODUCER_SCHEMA_VERSION",
    "PRODUCER_SHARD_SCHEMA_VERSION",
    "RECEIPT_SCHEMA_VERSION",
    "RepositoryIdentity",
    "StageCommand",
    "certification_catalog_identity",
    "deterministic_source_shard",
    "inspect_repository",
    "load_execution_binding",
    "load_secret_environment_bundle",
    "merge_evidence_shards",
    "produce_evidence",
    "verify_evidence_bundle",
]
