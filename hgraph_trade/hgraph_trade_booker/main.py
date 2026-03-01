"""
main.py

Entry point for the trade booking pipeline.

This script:
1. Parses command-line arguments for input file(s), output directory, etc.
2. Loads and validates each trade file.
3. Maps the trade data to the booking model.
4. Books the trade (writes to output directory), quarantining failures.
5. Reports a structured summary of the run.

Exit codes:
    0 — all trades processed successfully
    1 — one or more trades failed (partial success)
    2 — fatal error (nothing processed)
"""

import sys
import argparse
import glob
import logging

from hgraph_trade.logging_config import setup_logging
from hgraph_trade.hgraph_trade_booker.pipeline_result import (
    PipelineResult,
    TradeResult,
    TradeStatus,
)
from trade_loader import load_trade_from_file
from trade_mapper import map_trade_to_model
from trade_booker import book_trades_batch

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Process and book trades using specified input and output paths."
    )
    parser.add_argument(
        "--input_file",
        type=str,
        help="Path to a single input trade file (JSON/TXT).",
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        help="Directory containing trade files to process (*.json, *.txt). "
             "Mutually exclusive with --input_file.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory where booked trade files will be saved.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first error instead of continuing.",
    )
    return parser.parse_args()


def _collect_input_files(args: argparse.Namespace) -> list[str]:
    """Resolve the list of trade files to process."""
    if args.input_file and args.input_dir:
        logger.error("Specify --input_file OR --input_dir, not both.")
        sys.exit(2)

    if args.input_file:
        return [args.input_file]

    if args.input_dir:
        files = sorted(
            glob.glob(f"{args.input_dir}/*.json")
            + glob.glob(f"{args.input_dir}/*.txt")
        )
        if not files:
            logger.error("No trade files found in %s", args.input_dir)
            sys.exit(2)
        return files

    logger.error("Provide either --input_file or --input_dir.")
    sys.exit(2)


def main() -> None:
    """Run the trade booking pipeline."""
    args = _parse_args()

    setup_logging(level="DEBUG" if args.verbose else "INFO")

    input_files = _collect_input_files(args)
    pipeline = PipelineResult()

    # ------------------------------------------------------------------
    # Stage 1 & 2: Load, validate, and map each trade file
    # ------------------------------------------------------------------
    all_messages = []

    for file_path in input_files:
        trade_id = file_path  # best identifier until we parse the file
        try:
            logger.info("Loading trade data from: %s", file_path)
            trade_data = load_trade_from_file(file_path)
            trade_id = trade_data.get("trade_id", file_path)

            # Validate essential keys
            required_keys = {"instrument", "tradeType"}
            missing = required_keys - trade_data.keys()
            if missing:
                raise ValueError(f"Missing required keys: {missing}")

            logger.info("Mapping trade %s to model", trade_id)
            messages = map_trade_to_model(trade_data, fail_fast=args.fail_fast)

            if not messages:
                raise ValueError("Mapping produced zero trade messages")

            all_messages.extend(messages)
            pipeline.add(
                TradeResult(
                    trade_id=str(trade_id),
                    status=TradeStatus.SUCCESS,
                    message=f"Mapped {len(messages)} message(s)",
                    stage="mapping",
                    data=trade_data,
                )
            )

        except FileNotFoundError as exc:
            pipeline.add(
                TradeResult(
                    trade_id=str(trade_id),
                    status=TradeStatus.VALIDATION_FAILED,
                    message=str(exc),
                    error=exc,
                    stage="loading",
                )
            )
            logger.error("File not found: %s", exc)
            if args.fail_fast:
                break

        except ValueError as exc:
            pipeline.add(
                TradeResult(
                    trade_id=str(trade_id),
                    status=TradeStatus.VALIDATION_FAILED
                    if "Missing" in str(exc) or "Unsupported" in str(exc)
                    else TradeStatus.MAPPING_FAILED,
                    message=str(exc),
                    error=exc,
                    stage="validation" if "Missing" in str(exc) else "mapping",
                )
            )
            logger.error("Trade %s failed: %s", trade_id, exc)
            if args.fail_fast:
                break

        except Exception as exc:
            pipeline.add(
                TradeResult(
                    trade_id=str(trade_id),
                    status=TradeStatus.MAPPING_FAILED,
                    message=str(exc),
                    error=exc,
                    stage="mapping",
                )
            )
            logger.exception("Unexpected error processing %s", trade_id)
            if args.fail_fast:
                break

    # ------------------------------------------------------------------
    # Stage 3: Book all successfully mapped messages
    # ------------------------------------------------------------------
    if all_messages:
        logger.info("Booking %d trade message(s)", len(all_messages))
        result = book_trades_batch(all_messages, args.output_dir)

        if result["quarantined"]:
            for qpath in result["quarantined"]:
                pipeline.add(
                    TradeResult(
                        trade_id=qpath,
                        status=TradeStatus.BOOKING_FAILED,
                        message=f"Quarantined to {qpath}",
                        stage="booking",
                    )
                )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    pipeline.finalise()
    print("\n" + pipeline.summary())

    if pipeline.failure_count == 0:
        sys.exit(0)
    elif pipeline.success_count > 0:
        sys.exit(1)  # partial success
    else:
        sys.exit(2)  # total failure


if __name__ == "__main__":
    main()
