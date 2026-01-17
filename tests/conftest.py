"""Shared fixtures. Generates gRPC stubs on the fly if `make proto` was never run."""

from __future__ import annotations

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROTO = ROOT / "bidstorm" / "proto" / "bidding.proto"
GENERATED = ROOT / "bidstorm" / "proto" / "bidding_pb2_grpc.py"


def _ensure_stubs() -> None:
    if GENERATED.exists() and GENERATED.stat().st_mtime >= PROTO.stat().st_mtime:
        return
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I",
            str(ROOT),
            f"--python_out={ROOT}",
            f"--grpc_python_out={ROOT}",
            str(PROTO.relative_to(ROOT)),
        ],
        check=True,
        cwd=ROOT,
    )


_ensure_stubs()
