#!/usr/bin/env python3
"""
cli.py — Unified CLI for the hgraph_platform_tools project.

Provides a single entry point with subcommands for every module:

    python cli.py book    --input_file trade.json --output_dir output/
    python cli.py entitlements update trader1 Trader
    python cli.py entitlements query trader1
    python cli.py static-admin --init-db --db-path static_data.db
    python cli.py notify   --file trade.json
    python cli.py parse-xsd --xsd path/to/file.xsd

Can also be installed as a console script via pyproject.toml.
"""

import argparse
import logging
import sys

from hgraph_trade.logging_config import setup_logging

__all__ = ("main",)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subcommand: book
# ---------------------------------------------------------------------------
def _add_book_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("book", help="Process and book trades")
    p.add_argument("--input_file", type=str, help="Path to a single trade file")
    p.add_argument("--input_dir", type=str, help="Directory of trade files (*.json, *.txt)")
    p.add_argument("--output_dir", type=str, required=True, help="Output directory for booked trades")
    p.add_argument("--fail-fast", action="store_true", help="Stop on first error")
    p.add_argument("--verbose", action="store_true", help="Enable debug logging")
    p.set_defaults(func=_run_book)


def _run_book(args: argparse.Namespace) -> int:
    import glob as globmod
    from hgraph_trade.hgraph_trade_booker.pipeline_result import PipelineResult, TradeResult, TradeStatus
    from hgraph_trade.hgraph_trade_booker.trade_loader import load_trade_from_file
    from hgraph_trade.hgraph_trade_booker.trade_mapper import map_trade_to_model
    from hgraph_trade.hgraph_trade_booker.trade_booker import book_trades_batch

    files: list[str] = []
    if args.input_file:
        files = [args.input_file]
    elif args.input_dir:
        files = sorted(globmod.glob(f"{args.input_dir}/*.json") + globmod.glob(f"{args.input_dir}/*.txt"))
    else:
        logger.error("Provide --input_file or --input_dir")
        return 2

    if not files:
        logger.error("No trade files found")
        return 2

    pipeline = PipelineResult()
    all_messages = []

    for fp in files:
        trade_id = fp
        try:
            trade_data = load_trade_from_file(fp)
            trade_id = trade_data.get("trade_id", fp)
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
                )
            )
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
            logger.error("Trade %s failed: %s", trade_id, exc)
            if args.fail_fast:
                break

    if all_messages:
        result = book_trades_batch(all_messages, args.output_dir)
        if result["quarantined"]:
            for qp in result["quarantined"]:
                pipeline.add(
                    TradeResult(
                        trade_id=qp,
                        status=TradeStatus.BOOKING_FAILED,
                        message=f"Quarantined to {qp}",
                        stage="booking",
                    )
                )

    pipeline.finalise()
    print("\n" + pipeline.summary())
    return 0 if pipeline.failure_count == 0 else 1


# ---------------------------------------------------------------------------
# Subcommand: entitlements
# ---------------------------------------------------------------------------
def _add_entitlements_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("entitlements", help="Manage user entitlements")
    sp = p.add_subparsers(dest="ent_command", required=True)

    up = sp.add_parser("update", help="Update a user's role")
    up.add_argument("user_id", type=str, help="User identifier")
    up.add_argument("role", type=str, help="New role")

    qp = sp.add_parser("query", help="Query a user's role")
    qp.add_argument("user_id", type=str, help="User identifier")

    cp = sp.add_parser("check", help="Check if a user has a specific permission")
    cp.add_argument("user_id", type=str, help="User identifier")
    cp.add_argument("action", type=str, help="Action to check (e.g. execute_trade)")

    p.set_defaults(func=_run_entitlements)


def _run_entitlements(args: argparse.Namespace) -> int:
    from hgraph_entitlements import (
        STATIC_ROLES,
        get_db_connection,
        initialize_db,
        get_user_role,
        update_user_role,
        check_permission,
    )

    conn = get_db_connection()
    initialize_db(conn)

    if args.ent_command == "update":
        try:
            update_user_role(conn, args.user_id, args.role)
            logger.info("Updated user '%s' to role '%s'.", args.user_id, args.role)
        except ValueError as exc:
            logger.error("%s", exc)
            return 1

    elif args.ent_command == "query":
        role = get_user_role(conn, args.user_id)
        if role:
            logger.info("User '%s' has role '%s'. Allowed actions: %s", args.user_id, role, STATIC_ROLES.get(role))
        else:
            logger.warning("User '%s' not found.", args.user_id)
            return 1

    elif args.ent_command == "check":
        allowed = check_permission(args.user_id, args.action, conn=conn)
        if allowed:
            logger.info("User '%s' IS allowed to '%s'.", args.user_id, args.action)
        else:
            logger.info("User '%s' is NOT allowed to '%s'.", args.user_id, args.action)
            return 1

    conn.close()
    return 0


# ---------------------------------------------------------------------------
# Subcommand: static-admin
# ---------------------------------------------------------------------------
def _add_static_admin_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("static-admin", help="Manage static reference data")
    p.add_argument("--init-db", action="store_true", help="Initialise the database schema")
    p.add_argument("--fetch", action="store_true", help="Fetch and store static data from API")
    p.add_argument("--db-path", type=str, default=None, help="Path to SQLite database")
    p.add_argument("--api-url", type=str, default=None, help="API URL for static data")
    p.set_defaults(func=_run_static_admin)


def _run_static_admin(args: argparse.Namespace) -> int:
    from secure_config import config
    from hgraph_static_admin.example_code import init_db, run_pipeline

    db_path = args.db_path or config["STATIC_DATA_DB_PATH"]
    api_url = args.api_url or config["STATIC_DATA_API_URL"]

    if args.init_db:
        init_db(db_path)
        logger.info("Database initialised at %s", db_path)
    if args.fetch:
        run_pipeline(api_url, db_path)
    return 0


# ---------------------------------------------------------------------------
# Subcommand: notify
# ---------------------------------------------------------------------------
def _add_notify_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("notify", help="Send trade notification email")
    p.add_argument("--file", required=True, help="Path to JSON trade file")
    p.set_defaults(func=_run_notify)


def _run_notify(args: argparse.Namespace) -> int:
    from hgraph_notification.scripts.notification_email import (
        load_trade_data,
        event_driven_notification,
    )

    trade_info = load_trade_data(args.file)
    event_driven_notification(trade_info)
    return 0


# ---------------------------------------------------------------------------
# Subcommand: parse-xsd
# ---------------------------------------------------------------------------
def _add_parse_xsd_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("parse-xsd", help="Parse FpML XSD and generate JSON tag dictionary")
    p.add_argument("--xsd", type=str, default=None, help="Path to XSD file")
    p.add_argument("--output", type=str, default=None, help="Output JSON file path")
    p.set_defaults(func=_run_parse_xsd)


def _run_parse_xsd(args: argparse.Namespace) -> int:
    from hgraph_trade.fpml_xsd_reference_files.fpml_XSD_parser import (
        parse_xsd_schema,
        save_fpml_tags,
        DEFAULT_XSD,
        DEFAULT_OUTPUT,
    )

    xsd = args.xsd or DEFAULT_XSD
    output = args.output or DEFAULT_OUTPUT
    fpml_tags = parse_xsd_schema(xsd)
    save_fpml_tags(fpml_tags, output)
    print(f"Done. {len(fpml_tags)} elements written to {output}")
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="hgraph",
        description="hgraph_platform_tools — unified command-line interface",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging (applies to all subcommands)",
    )

    subparsers = parser.add_subparsers(dest="command")
    _add_book_parser(subparsers)
    _add_entitlements_parser(subparsers)
    _add_static_admin_parser(subparsers)
    _add_notify_parser(subparsers)
    _add_parse_xsd_parser(subparsers)

    args = parser.parse_args()

    # Global verbose flag (subcommand-level --verbose also works for book)
    verbose = args.verbose or getattr(args, "verbose", False)
    setup_logging(level="DEBUG" if verbose else "INFO")

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    exit_code = args.func(args)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
