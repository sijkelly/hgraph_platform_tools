"""Tests for the order parser evaluation's grading (no model is called)."""

import json
import os
from datetime import date

import pytest

from hgraph_api.services.nlp_order_parser import ORDER_FIELDS
from hgraph_api.services.parser_eval import EvalCase, grade_case, grade_field, load_cases, run_cases, summarise

CASES_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "evals", "order_parser", "cases.jsonl")


def _case(**expected) -> EvalCase:
    return EvalCase(
        id="c1",
        text="buy 10k HH Jan 2027 at 3.5 from ACME",
        expected={**{name: None for name in ORDER_FIELDS}, **expected},
        today=date(2026, 11, 13),
        counterparties=("ACME",),
    )


def _parsed(**values) -> dict:
    return {name: {"value": value, "confidence": 0.9} for name, value in values.items()}


@pytest.mark.parametrize(
    "name,expected,got,correct",
    [
        ("quantity", "10000", "10,000", True),
        ("quantity", "10000", "10000.0", True),
        ("quantity", "10000", "1000", False),
        ("quantity", "10000", "ten thousand", False),
        ("price", "3.5", "3.50", True),
        ("price", "3.5", "3.51", False),
        ("side", "Buy", "buy", True),
        ("side", "Buy", "Sell", False),
        ("counterparty", "ACME", " acme ", True),
        ("counterparty", "ACME", "GLOBEX", False),
        ("delivery_period", "January 2027", "Jan 2027", True),
        ("delivery_period", "January 2027", "2027-01", True),
        ("delivery_period", "Cal 2027", "2027", True),
        ("delivery_period", "January 2027", "January 2028", False),
        ("delivery_period", "January 2027", "soon", False),
        ("delivery_period", "balance of month", "Balance of Month", True),
        ("instrument", "Henry Hub Natural Gas", "henry hub natural gas", True),
        ("instrument", "Henry Hub Natural Gas", "TTF Natural Gas", False),
        ("instrument", "Rotterdam coal", "API2 Rotterdam coal", True),
        ("instrument", "Rotterdam coal", "Henry Hub Natural Gas", False),
        ("counterparty", None, None, True),
        ("counterparty", None, "", True),
        ("counterparty", None, "ACME", False),
        ("quantity", "10000", None, False),
        ("quantity", "10000", "", False),
    ],
)
def test_grade_field(name, expected, got, correct):
    assert grade_field(name, expected, got) is correct


def test_case_passes_only_when_every_field_is_right():
    case = _case(instrument="Henry Hub Natural Gas", side="Buy", quantity="10000", price="3.5")
    right = _parsed(instrument="Henry Hub Natural Gas", side="Buy", quantity="10,000", price="3.50")

    assert grade_case(case, right).passed
    wrong_price = grade_case(case, {**right, **_parsed(price="3.6")})
    assert not wrong_price.passed
    assert [name for name, ok in wrong_price.fields.items() if not ok] == ["price"]


def test_inventing_a_field_fails_the_case():
    case = _case(instrument="Henry Hub Natural Gas", side="Buy")
    result = grade_case(case, _parsed(instrument="Henry Hub Natural Gas", side="Buy", counterparty="ACME"))
    assert not result.passed
    assert result.fields["counterparty"] is False


def test_parser_is_called_with_the_cases_inputs_and_errors_are_recorded():
    calls = []

    def parse(text, context, *, counterparties, today):
        calls.append((text, context, counterparties, today))
        if "boom" in text:
            raise RuntimeError("model unavailable")
        return _parsed(side="Buy")

    good = _case(side="Buy")
    bad = EvalCase(id="c2", text="boom", expected=good.expected, today=good.today)
    results = run_cases([good, bad], parse)

    assert calls[0] == (good.text, None, ("ACME",), date(2026, 11, 13))
    assert results[0].passed
    assert not results[1].passed
    assert results[1].error == "RuntimeError: model unavailable"


def test_summary_scores_cases_and_fields():
    case = _case(side="Buy", quantity="10000")
    results = [
        grade_case(case, _parsed(side="Buy", quantity="10000")),
        grade_case(case, _parsed(side="Sell", quantity="10000")),
    ]
    summary = summarise(results)

    assert (summary["cases"], summary["cases_passed"]) == (2, 1)
    assert summary["per_field"]["side"] == 0.5
    assert summary["per_field"]["quantity"] == 1.0
    assert summary["field_accuracy"] == pytest.approx((len(ORDER_FIELDS) - 0.5) / len(ORDER_FIELDS))
    assert summary["failures"] == [{"id": "c1", "error": "", "wrong": {"side": {"expected": "Buy", "got": "Sell"}}}]


def test_summary_of_nothing():
    assert summarise([])["field_accuracy"] == 0.0


def test_shipped_cases_load_and_a_perfect_parser_scores_full_marks():
    cases = load_cases(CASES_FILE)
    assert len(cases) >= 20
    by_text = {case.text + str(case.context): case for case in cases}

    def perfect(text, context, *, counterparties, today):
        expected = by_text[text + str(context)].expected
        return _parsed(**{name: value for name, value in expected.items() if value is not None})

    summary = summarise(run_cases(cases, perfect))
    assert summary["cases_passed"] == summary["cases"]
    assert summary["field_accuracy"] == 1.0


def test_shipped_cases_catch_a_parser_that_fills_every_gap():
    cases = load_cases(CASES_FILE)

    def gap_filler(text, context, *, counterparties, today):
        return _parsed(**{name: "ACME" for name in ORDER_FIELDS})

    assert summarise(run_cases(cases, gap_filler))["cases_passed"] == 0


@pytest.mark.parametrize(
    "line,message",
    [
        ('{"id": "a", "text": "x", "today": "2026-11-13"}', "expected"),
        ('{"id": "a", "text": "x", "today": "not a date", "expected": {}}', "line 1"),
        ('{"id": "a", "text": "x", "today": "2026-11-13", "expected": {"colour": "red"}}', "unknown fields"),
        ("not json", "line 1"),
    ],
)
def test_malformed_cases_are_refused(tmp_path, line, message):
    path = tmp_path / "cases.jsonl"
    path.write_text(line + "\n")
    with pytest.raises(ValueError, match=message):
        load_cases(str(path))


def test_duplicate_case_ids_are_refused(tmp_path):
    line = json.dumps({"id": "a", "text": "x", "today": "2026-11-13", "expected": {}})
    path = tmp_path / "cases.jsonl"
    path.write_text(f"{line}\n{line}\n")
    with pytest.raises(ValueError, match="duplicate"):
        load_cases(str(path))
