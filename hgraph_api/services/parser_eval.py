"""
Grading for the order parser evaluation.

A case gives a message and the value each order field should come out with
(``None`` where the message does not give one).  Grading is exact per field
after normalisation and uses no model, so a score means the same thing on
every run:

* numbers compare as numbers (``"3.50"`` equals ``"3.5"``),
* text ignores case and surrounding whitespace,
* delivery periods compare by the dates they cover (``"Jan 2027"`` equals ``"January 2027"``),
* an instrument expected to be outside the product list is correct as long as the parser did
  not map it onto a known product,
* a field expected to be ``None`` is correct only if the parser left it out.

The cases live in ``evals/order_parser/cases.jsonl``; ``scripts/eval_order_parser.py`` runs them.
"""

import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Iterable

from hgraph_api.services.nlp_order_parser import ORDER_FIELDS
from hgraph_api.services.order_engine import parse_number
from hgraph_oms.trades import DEFAULT_PRODUCTS, ProductSpec, find_product, parse_delivery_period

__all__ = ("EvalCase", "CaseResult", "load_cases", "grade_field", "grade_case", "run_cases", "summarise")

_NUMERIC_FIELDS = ("quantity", "price")


@dataclass(frozen=True, kw_only=True)
class EvalCase:
    """One message and what the parser should make of it.

    :param expected: Expected value per order field; ``None`` means the field must be left out.
    :param today: The date periods without a year are resolved against.
    """

    id: str
    text: str
    expected: dict[str, str | None]
    today: date
    counterparties: tuple[str, ...] = ()
    context: str | None = None
    note: str = ""


@dataclass(frozen=True, kw_only=True)
class CaseResult:
    """How one case was graded.

    :param fields: Per field, whether the parser's value was correct.
    :param got: The parser's value per field (``None`` if it left the field out).
    :param error: Why the parser produced nothing, if it failed outright; every field is then wrong.
    """

    case: EvalCase
    fields: dict[str, bool] = field(default_factory=dict)
    got: dict[str, str | None] = field(default_factory=dict)
    error: str = ""

    @property
    def passed(self) -> bool:
        return not self.error and all(self.fields.values())


def load_cases(path: str) -> list[EvalCase]:
    """Read cases from a JSON-lines file.

    :raises ValueError: If a case is malformed, names an unknown field, or repeats an ID.
    """
    cases: list[EvalCase] = []
    seen: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for number, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                expected = {name: raw["expected"].get(name) for name in ORDER_FIELDS}
                unknown = set(raw["expected"]) - set(ORDER_FIELDS)
                if unknown:
                    raise ValueError(f"unknown fields {sorted(unknown)}")
                case = EvalCase(
                    id=raw["id"],
                    text=raw["text"],
                    expected=expected,
                    today=date.fromisoformat(raw["today"]),
                    counterparties=tuple(raw.get("counterparties", ())),
                    context=raw.get("context"),
                    note=raw.get("note", ""),
                )
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise ValueError(f"{path} line {number}: {exc}") from exc
            if case.id in seen:
                raise ValueError(f"{path} line {number}: duplicate case id '{case.id}'")
            seen.add(case.id)
            cases.append(case)
    return cases


def _is_known_product(name: str, products: Iterable[ProductSpec]) -> bool:
    try:
        find_product(name, products)
    except ValueError:
        return False
    return True


def grade_field(
    name: str, expected: str | None, got: str | None, products: Iterable[ProductSpec] = DEFAULT_PRODUCTS
) -> bool:
    """Return whether the parser's value for one field is correct."""
    got = got.strip() if isinstance(got, str) else got
    if expected is None:
        return not got
    if not got:
        return False

    if name in _NUMERIC_FIELDS:
        try:
            return parse_number(got, name) == parse_number(expected, name)
        except ValueError:
            return False
    if name == "delivery_period":
        try:
            return parse_delivery_period(got) == parse_delivery_period(expected)
        except ValueError:
            return got.lower() == expected.lower()
    if name == "instrument":
        products = tuple(products)
        if not _is_known_product(expected, products):
            # Any faithful description will do, so long as it was not turned into a product we book
            return not _is_known_product(got, products)
    return got.lower() == expected.strip().lower()


def grade_case(
    case: EvalCase, parsed: dict[str, Any], products: Iterable[ProductSpec] = DEFAULT_PRODUCTS
) -> CaseResult:
    """Grade the parser's output for one case."""
    products = tuple(products)
    got: dict[str, str | None] = {}
    for name in ORDER_FIELDS:
        value = parsed.get(name)
        got[name] = str(value.get("value", "")) if isinstance(value, dict) else None
    fields = {name: grade_field(name, case.expected[name], got[name], products) for name in ORDER_FIELDS}
    return CaseResult(case=case, fields=fields, got=got)


def run_cases(cases: Iterable[EvalCase], parse: Callable[..., dict[str, Any]]) -> list[CaseResult]:
    """Run the parser over every case and grade it.

    :param parse: Called like :func:`~hgraph_api.services.nlp_order_parser.parse_order_text`.
        A case the parser fails on is recorded with the error, not raised.
    """
    results = []
    for case in cases:
        try:
            parsed = parse(case.text, case.context, counterparties=case.counterparties, today=case.today)
        except Exception as exc:
            results.append(
                CaseResult(
                    case=case, fields={name: False for name in ORDER_FIELDS}, error=f"{type(exc).__name__}: {exc}"
                )
            )
            continue
        results.append(grade_case(case, parsed))
    return results


def summarise(results: Iterable[CaseResult]) -> dict[str, Any]:
    """Return the scores for a run.

    :returns: ``cases`` and ``cases_passed`` (every field right), ``field_accuracy`` (share of all
        fields right), ``per_field`` (accuracy by field) and ``failures`` (what was wrong, per case).
    """
    results = list(results)
    total = len(results)
    per_field = {
        name: (sum(r.fields.get(name, False) for r in results) / total if total else 0.0) for name in ORDER_FIELDS
    }
    failures = [
        {
            "id": r.case.id,
            "error": r.error,
            "wrong": {
                name: {"expected": r.case.expected[name], "got": r.got.get(name)}
                for name in ORDER_FIELDS
                if not r.fields.get(name, False)
            },
        }
        for r in results
        if not r.passed
    ]
    return {
        "cases": total,
        "cases_passed": sum(r.passed for r in results),
        "field_accuracy": sum(per_field.values()) / len(ORDER_FIELDS) if total else 0.0,
        "per_field": per_field,
        "failures": failures,
    }
