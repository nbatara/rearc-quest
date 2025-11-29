#!/usr/bin/env python3
"""Convert CloudFormation parameter JSON to CLI override arguments."""

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Dict, Iterable, Tuple


def _load_parameters(path: Path) -> Dict[str, str]:
    """Load parameters from either {"Parameters": {...}} or a list format."""
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if isinstance(data, dict) and "Parameters" in data:
        params = data["Parameters"]
        if not isinstance(params, dict):
            raise ValueError("Parameters entry must be an object.")
        return {str(k): str(v) for k, v in params.items()}

    if isinstance(data, list):
        extracted: Dict[str, str] = {}
        for entry in data:
            if not isinstance(entry, dict):
                raise ValueError("List entries must be objects.")
            key = entry.get("ParameterKey")
            value = entry.get("ParameterValue")
            if key is None or value is None:
                raise ValueError("Each entry needs ParameterKey and ParameterValue.")
            extracted[str(key)] = str(value)
        return extracted

    raise ValueError("Unsupported JSON format for CloudFormation parameters.")


def _format_overrides(items: Iterable[Tuple[str, str]]) -> str:
    """Return a space-separated list of CLI-safe Key=Value pairs."""
    return " ".join(f"{key}={shlex.quote(value)}" for key, value in items)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Convert CloudFormation parameter JSON to a space-separated list "
            "of Key=Value arguments suitable for --parameter-overrides."
        )
    )
    parser.add_argument(
        "input",
        metavar="PATH",
        help="Path to the JSON file containing CloudFormation parameters.",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="DEST",
        help="Optional path to write the converted overrides; defaults to stdout.",
    )
    args = parser.parse_args()

    try:
        params = _load_parameters(Path(args.input))
    except (OSError, ValueError, json.JSONDecodeError) as err:
        print(f"Error reading parameters: {err}", file=sys.stderr)
        sys.exit(1)

    overrides = _format_overrides(sorted(params.items()))

    if args.output:
        Path(args.output).write_text(overrides + "\n", encoding="utf-8")
    else:
        print(overrides)


if __name__ == "__main__":
    main()
