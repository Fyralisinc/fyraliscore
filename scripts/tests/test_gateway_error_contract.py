from __future__ import annotations

from pathlib import Path

from scripts.check_gateway_error_contract import (
    iter_gateway_files,
    validate_gateway_error_contract,
)


def test_checked_in_gateway_error_contract_passes() -> None:
    assert validate_gateway_error_contract(iter_gateway_files()) == []


def test_gateway_error_contract_rejects_raw_exception_text(tmp_path: Path) -> None:
    route = tmp_path / "route.py"
    route.write_text(
        """
from fastapi import HTTPException
from fastapi.responses import JSONResponse

def handler():
    try:
        raise RuntimeError("raw sql")
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

def other_handler():
    try:
        raise RuntimeError("secret")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"failed: {e}") from e
""",
        encoding="utf-8",
    )

    violations = validate_gateway_error_contract([route])

    assert len(violations) == 2
    assert {violation.message for violation in violations} == {
        "response content must not expose raw exception text",
    }


def test_gateway_error_contract_rejects_implementation_details(
    tmp_path: Path,
) -> None:
    route = tmp_path / "route.py"
    route.write_text(
        """
from fastapi import HTTPException
from fastapi.responses import JSONResponse

def handler():
    return JSONResponse({"error": "database_pool_unavailable"}, status_code=503)

def other_handler():
    raise HTTPException(status_code=503, detail="gateway deps unavailable")
""",
        encoding="utf-8",
    )

    violations = validate_gateway_error_contract([route])

    assert len(violations) == 2
    assert {violation.message for violation in violations} == {
        "response content must not expose implementation details",
    }


def test_gateway_error_contract_allows_bounded_codes(tmp_path: Path) -> None:
    route = tmp_path / "route.py"
    route.write_text(
        """
from fastapi import HTTPException
from fastapi.responses import JSONResponse

def handler():
    try:
        raise RuntimeError("raw sql")
    except Exception as exc:
        return JSONResponse({"error": "internal_error"}, status_code=500)

def other_handler():
    try:
        raise RuntimeError("secret")
    except Exception as e:
        raise HTTPException(status_code=400, detail="invalid_request") from e
""",
        encoding="utf-8",
    )

    assert validate_gateway_error_contract([route]) == []
