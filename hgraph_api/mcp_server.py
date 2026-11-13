"""
MCP server that gives an AI agent access to the OMS.

The server is a client of the REST API, so an agent goes through the same
authentication, entitlement checks and order engine as any other caller, and
is identified by its own user ID.  It can read orders, trades, credit and
reference data, and it can *draft* an order as a proposal.  There is
deliberately no tool that submits, amends, fills or cancels an order: a person
confirms a proposal through the API or UI, and that person must not be the
agent.

Run it with ``hgraph-tools mcp --api-url http://localhost:8000 --user-id ai-agent``.
"""

import functools
from typing import Any, Callable

import httpx

from hgraph_oms.trades import DEFAULT_PRODUCTS

__all__ = ("TOOL_NAMES", "OmsApiError", "OmsApiTools", "build_server")

# The complete set of things an agent can do. Nothing here submits or changes an order.
TOOL_NAMES = (
    "list_orders",
    "get_order",
    "get_order_trades",
    "list_counterparties",
    "get_credit",
    "list_products",
    "propose_order",
    "list_proposals",
)

_INSTRUCTIONS = """Tools for a commodity trading desk's order management system.

You can look up orders, the trades booked from their fills, counterparties, credit and the \
products the desk trades. You can draft an order with propose_order. A proposal does nothing \
until a person on the desk confirms it; you cannot submit, change, fill or cancel orders, so \
never tell a user an order has been placed. Before proposing, check the counterparty's credit \
and that the instrument is one of the listed products, and say in the rationale how you read \
the request and anything you were unsure of."""


class OmsApiError(RuntimeError):
    """The OMS API refused a request or could not be reached. The message says why."""


class OmsApiTools:
    """The operations an agent may perform, implemented over the OMS REST API.

    :param http: An HTTP client whose base URL is the OMS API and whose headers identify the
        agent (``X-User-Id``).  Anything with httpx's ``request`` method works.
    """

    def __init__(self, http: Any):
        self._http = http

    def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise OmsApiError(f"The OMS API could not be reached: {type(exc).__name__}") from exc
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise OmsApiError(f"{detail} (HTTP {response.status_code})")
        return response.json()

    def list_orders(self, status: str = "", counterparty: str = "") -> list[dict[str, Any]]:
        """List orders, most recent first.

        Each order has its terms, status (Pending, Working, Partially Filled, Filled, Rejected or
        Cancelled), filled quantity, average fill price and, if rejected or cancelled, the reason.

        :param status: Only orders in this status. Empty for all.
        :param counterparty: Only orders with this counterparty symbol. Empty for all.
        """
        orders = self._call("GET", "/api/v1/orders")
        return [
            order
            for order in orders
            if (not status or order["status"] == status) and (not counterparty or order["counterparty"] == counterparty)
        ]

    def get_order(self, order_id: str) -> dict[str, Any]:
        """Get one order by its ID (for example ORD-1A2B3C4D)."""
        return self._call("GET", f"/api/v1/orders/{order_id}")

    def get_order_trades(self, order_id: str) -> list[dict[str, Any]]:
        """List the trades created from an order's fills.

        Each trade shows whether it was booked (or why booking failed) and whether it has been
        delivered to the downstream trade system yet.
        """
        return self._call("GET", f"/api/v1/orders/{order_id}/trades")

    def list_counterparties(self) -> list[dict[str, Any]]:
        """List the counterparties the desk can trade with, with their symbols."""
        return self._call("GET", "/api/v1/parties/entities")

    def get_credit(self, counterparty: str) -> dict[str, Any]:
        """Get a counterparty's credit limit, the amount utilised and the amount available.

        New orders are refused when their notional (quantity x price) would take utilisation plus
        the counterparty's other open orders over the limit.  A counterparty with no limit
        configured is not credit checked.

        :param counterparty: The counterparty symbol, for example ACME.
        """
        try:
            return self._call("GET", f"/api/v1/credit/utilizations/{counterparty}")
        except OmsApiError as exc:
            if "HTTP 404" in str(exc):
                return {"counterparty_symbol": counterparty, "note": "No credit limit is configured"}
            raise

    def list_products(self) -> list[dict[str, str]]:
        """List the products the desk can book, with the exact instrument name to use in an order.

        An order for an instrument that is not listed can be worked but its trades will not book.
        """
        return [
            {"instrument": p.name, "price_index": p.commodity_reference, "currency": p.currency, "asset": p.asset}
            for p in DEFAULT_PRODUCTS
        ]

    def propose_order(
        self,
        instrument: str,
        side: str,
        quantity: str,
        counterparty: str,
        delivery_period: str,
        rationale: str,
        price: str = "",
        unit: str = "",
        portfolio: str = "",
        book: str = "",
        source_text: str = "",
    ) -> dict[str, Any]:
        """Draft an order for a person on the desk to confirm. This does not place the order.

        Returns the proposal with its ID and status "Proposed".  It stays that way until a person
        confirms it (which submits the order under their name, subject to the credit check) or
        dismisses it.

        :param instrument: Product name exactly as given by list_products.
        :param side: "Buy" or "Sell", from the desk's point of view.
        :param quantity: Total quantity for the delivery period, digits only (for example "50000").
        :param counterparty: Counterparty symbol as given by list_counterparties.
        :param delivery_period: A month ("January 2027"), a quarter ("Q3 2026") or a year ("Cal 2027").
        :param rationale: How you read the request, and anything you were unsure of. The person
            deciding sees this.
        :param price: Limit price per unit. Leave empty for a market order.
        :param unit: Unit of the quantity, for example MMBtu, MWh or bbl.
        :param portfolio: Portfolio to book into, if known.
        :param book: Book to book into, if known.
        :param source_text: The original message the order comes from, word for word.
        """
        return self._call(
            "POST",
            "/api/v1/proposals",
            json={
                "instrument": instrument,
                "side": side,
                "quantity": quantity,
                "unit": unit,
                "price": price,
                "counterparty": counterparty,
                "delivery_period": delivery_period,
                "order_type": "Limit" if price.strip() else "Market",
                "portfolio": portfolio,
                "book": book,
                "source_text": source_text,
                "rationale": rationale,
            },
        )

    def list_proposals(self, status: str = "") -> list[dict[str, Any]]:
        """List order proposals, newest first, with what was decided about each.

        :param status: "Proposed" (awaiting a decision), "Confirmed" or "Dismissed". Empty for all.
        """
        return self._call("GET", "/api/v1/proposals", params={"status": status} if status else None)


def build_server(tools: OmsApiTools):
    """Return an MCP server exposing the agent's tools."""
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError

    def reporting_refusals(tool: Callable[..., Any]) -> Callable[..., Any]:
        # MCP only passes a tool's error text on to the model when it is a ToolError; the
        # API's reason for refusing ("credit limit exceeded", "not found") is what the agent needs
        @functools.wraps(tool)
        def call(*args: Any, **kwargs: Any) -> Any:
            try:
                return tool(*args, **kwargs)
            except OmsApiError as exc:
                raise ToolError(str(exc)) from exc

        return call

    server = MCPServer(name="hgraph-oms", instructions=_INSTRUCTIONS)
    for name in TOOL_NAMES:
        server.add_tool(reporting_refusals(getattr(tools, name)), name=name)
    return server
