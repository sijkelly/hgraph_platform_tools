"""Run the order parser evaluation.

Usage:
    uv run python scripts/eval_order_parser.py --dry-run
    uv run python scripts/eval_order_parser.py --yes [--limit 5] [--output results.json] [--min-accuracy 0.9]

Each case is one Claude API call (model: ORDER_PARSER_MODEL, default claude-opus-5-5), which costs
money, so nothing is sent without --yes.  See evals/order_parser/README.md.
"""

import argparse
import json
import os
import sys
from functools import partial

from hgraph_api.services.nlp_order_parser import DEFAULT_MODEL, ORDER_FIELDS, parse_order_text
from hgraph_api.services.parser_eval import load_cases, run_cases, summarise

DEFAULT_CASES = os.path.join(os.path.dirname(__file__), "..", "evals", "order_parser", "cases.jsonl")


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the order parser against labelled messages.")
    parser.add_argument("--cases", default=DEFAULT_CASES, help="JSON-lines file of cases")
    parser.add_argument("--limit", type=int, default=None, help="Only run the first N cases")
    parser.add_argument(
        "--dry-run", action="store_true", help="List the cases and the number of API calls; send nothing"
    )
    parser.add_argument("--yes", action="store_true", help="Confirm that the API calls may be made")
    parser.add_argument("--output", default=None, help="Write the full results to this JSON file")
    parser.add_argument("--min-accuracy", type=float, default=None, help="Exit 1 if field accuracy is below this (0-1)")
    args = parser.parse_args()

    cases = load_cases(args.cases)[: args.limit]
    model = os.getenv("ORDER_PARSER_MODEL") or DEFAULT_MODEL
    print(f"{len(cases)} case(s) from {os.path.relpath(args.cases)}; one API call each to {model}")

    if args.dry_run:
        for case in cases:
            print(f"  {case.id}: {case.text[:90]!r}")
        return 0
    if not args.yes:
        print("Nothing sent. This makes paid API calls; re-run with --yes to go ahead, or --dry-run to list the cases.")
        return 2

    usage: dict[str, int] = {}
    results = run_cases(cases, partial(parse_order_text, usage=usage))
    summary = summarise(results)

    print(f"\nCases fully correct: {summary['cases_passed']}/{summary['cases']}")
    print(f"Field accuracy:      {summary['field_accuracy']:.1%}")
    for name in ORDER_FIELDS:
        print(f"  {name:<16} {summary['per_field'][name]:.0%}")
    for failure in summary["failures"]:
        print(f"\n{failure['id']}" + (f": {failure['error']}" if failure["error"] else ""))
        for name, values in failure["wrong"].items():
            if not failure["error"]:
                print(f"  {name}: expected {values['expected']!r}, got {values['got']!r}")
    print(f"\nTokens used: {usage.get('input_tokens', 0)} in, {usage.get('output_tokens', 0)} out")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump({"model": model, "usage": usage, **summary}, fh, indent=2)
        print(f"Results written to {args.output}")

    if args.min_accuracy is not None and summary["field_accuracy"] < args.min_accuracy:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
