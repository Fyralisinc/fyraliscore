#!/usr/bin/env python3
"""Run the 3000-signal company-scale end-to-end probe.

This is a named entrypoint for the generalized company probe implemented
in `run_1000_signal_model_layer_probe.py`. That older filename remains
for compatibility with previous reports; the default signal count is now
3000 and can still be overridden with `--signals`.
"""
from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    target = Path(__file__).with_name("run_1000_signal_model_layer_probe.py")
    runpy.run_path(str(target), run_name="__main__")
