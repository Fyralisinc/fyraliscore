from __future__ import annotations

from pathlib import Path

from scripts.check_product_error_contract import (
    iter_product_files,
    validate_product_error_contract,
)


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "route.py"
    path.write_text(content)
    return path


def test_checked_in_product_error_contract_passes() -> None:
    violations = validate_product_error_contract(iter_product_files())
    assert violations == []


def test_rejects_raw_exception_detail(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
from fastapi import HTTPException

def route(exc):
    raise HTTPException(status_code=400, detail=str(exc))
""",
    )

    violations = validate_product_error_contract([path])

    assert len(violations) == 1
    assert "bounded error code" in violations[0].message


def test_rejects_freeform_literal_detail(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
from fastapi import HTTPException

def route():
    raise HTTPException(status_code=400, detail="x-tenant-id header required")
""",
    )

    violations = validate_product_error_contract([path])

    assert len(violations) == 1
    assert "bounded error code" in violations[0].message


def test_rejects_implementation_detail_terms(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
from fastapi import HTTPException
from fastapi.responses import JSONResponse

def route():
    if True:
        raise HTTPException(status_code=503, detail="gateway_deps_not_initialised")
    return JSONResponse({"error": "database_pool_unavailable"}, status_code=503)
""",
    )

    violations = validate_product_error_contract([path])

    assert len(violations) == 2
    assert {violation.message for violation in violations} == {
        "response content must not expose implementation details",
    }


def test_allows_bounded_codes_and_structured_dependency_detail(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
from fastapi import HTTPException

def route(exc, field):
    if field:
        raise HTTPException(status_code=400, detail=f"invalid_{field}")
    raise HTTPException(
        status_code=503,
        detail={"error": exc.code, "dependency": "rendering"},
    )
""",
    )

    violations = validate_product_error_contract([path])

    assert violations == []


def test_rejects_raw_json_response_error(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
from fastapi.responses import JSONResponse

def route(exc):
    return JSONResponse({"error": str(exc)}, status_code=400)
""",
    )

    violations = validate_product_error_contract([path])

    assert len(violations) == 1
    assert "raw exception text" in violations[0].message
