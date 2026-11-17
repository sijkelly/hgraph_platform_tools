# Order parser evaluation

Measures how accurately `POST /api/v1/orders/parse-nlp` (`hgraph_api/services/nlp_order_parser.py`) turns
free text into order fields.

- `cases.jsonl` — one case per line: the message, today's date, the known counterparties, optional UI
  context, and the expected value of every field (`null` where the message does not give one).
- Grading is exact, per field, after normalisation: numbers compare as numbers, text ignores case and
  surrounding space, delivery periods compare by the dates they cover. A field expected to be `null` is
  only correct if the parser leaves it out, so inventing a value counts as an error. Grading uses no model.

**The cases here are synthetic.** They were written to cover the situations the parser prompt describes
(shorthand, client-side messages, missing years, unknown products, embedded instructions), not taken from
real order flow. Replace or extend them with real, anonymised messages before relying on the numbers.

```bash
# Show what would run and how many API calls it makes, without calling the API
uv run python scripts/eval_order_parser.py --dry-run

# Run it (one Claude API call per case; needs ANTHROPIC_API_KEY)
uv run python scripts/eval_order_parser.py --yes --output evals/order_parser/results.json
```

Each run makes one API call per case with the model in `ORDER_PARSER_MODEL` (default `claude-opus-5-5`).
The script prints token usage at the end, which is what the run cost.
