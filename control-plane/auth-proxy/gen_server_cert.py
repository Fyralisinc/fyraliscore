#!/usr/bin/env python3
"""gen_server_cert.py — mint the auth-proxy's OWN server (serverAuth) cert.

The proxy verifies *clients* against the CA chain, but it also needs to *present*
a server cert so the mTLS handshake completes and clients can verify the proxy.
That server cert is signed by the same Fyralis intermediate (so a client that
trusts the CA root trusts the proxy too) and carries the proxy's DNS/IP SANs.

This is a small operational convenience for local/compose runs; in production the
proxy's server cert is issued like any other service cert. It writes:

    <out-dir>/proxy-server.crt   (serverAuth leaf)
    <out-dir>/proxy-server.key   (private key, 0600)

Usage:
    python gen_server_cert.py --san localhost --san 127.0.0.1 --san auth-proxy
    python gen_server_cert.py --pki-dir ../ca/pki --out-dir ./tls
"""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import os
import stat
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CA_DIR = _HERE.parent / "ca"
sys.path.insert(0, str(_CA_DIR))

import ca_lib  # noqa: E402
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID  # noqa: E402

DEFAULT_PKI_DIR = _CA_DIR / "pki"
DEFAULT_OUT_DIR = _HERE / "tls"


def _san_entry(value: str):
    try:
        return x509.IPAddress(ipaddress.ip_address(value))
    except ValueError:
        return x509.DNSName(value)


def gen(pki_dir: Path, out_dir: Path, sans: list[str], valid_days: int) -> dict:
    inter_crt = pki_dir / "intermediate.crt"
    inter_key = pki_dir / "keys" / "intermediate.key"
    for p in (inter_crt, inter_key):
        if not p.exists():
            raise SystemExit(f"missing CA material {p} — run ca/bootstrap_ca.py first")
    intermediate = ca_lib.CertKeyPair(
        cert=ca_lib.load_cert(inter_crt.read_bytes()),
        key=ca_lib.load_key(inter_key.read_bytes()),
    )

    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fyralis-auth-proxy")]))
        .issuer_name(intermediate.cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=valid_days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.SubjectAlternativeName([_san_entry(s) for s in sans]), critical=False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(
                intermediate.key.public_key()
            ),
            critical=False,
        )
        .sign(private_key=intermediate.key, algorithm=hashes.SHA256())
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    crt_path = out_dir / "proxy-server.crt"
    key_path = out_dir / "proxy-server.key"
    crt_path.write_bytes(ca_lib.cert_to_pem(cert))
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)
    return {"cert": str(crt_path), "key": str(key_path), "sans": sans}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Mint the auth-proxy server cert.")
    ap.add_argument("--pki-dir", default=str(DEFAULT_PKI_DIR))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--valid-days", type=int, default=90)
    ap.add_argument(
        "--san",
        action="append",
        default=None,
        help="DNS name or IP SAN (repeatable). Default: localhost 127.0.0.1 auth-proxy",
    )
    args = ap.parse_args(argv)
    sans = args.san or ["localhost", "127.0.0.1", "auth-proxy"]
    res = gen(Path(args.pki_dir), Path(args.out_dir), sans, args.valid_days)
    print("Issued auth-proxy server cert.")
    print("  cert: %s" % res["cert"])
    print("  key:  %s" % res["key"])
    print("  SANs: %s" % ", ".join(res["sans"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
