"""Tests for the MCP server: the agent's tools run against the real API through a test client."""

import asyncio
import json
import os

import pytest
from fastapi.testclient import TestClient

from hgraph_api.app import create_app
from hgraph_api.mcp_server import OmsApiError, OmsApiTools, build_server

_TRADER = {"X-User-Id": "trader1"}

_ORDER = {
    "instrument": "Henry Hub Natural Gas",
    "side": "Buy",
    "quantity": "50000",
    "unit": "MMBtu",
    "price": "3.45",
    "counterparty": "ACME",
    "delivery_period": "January 2027",
}


@pytest.fixture()
def api(tmp_path):
    """The OMS API, called as the agent's own user."""
    app = create_app()
    app.state.db_dir = str(tmp_path)
    with TestClient(app, headers={"X-User-Id": "ai-agent"}) as client:
        yield client


@pytest.fixture()
def tools(api) -> OmsApiTools:
    return OmsApiTools(api)


def _call(server, name, arguments):
    """Call a tool through the MCP server and return its result as Python data."""
    result = asyncio.run(server.call_tool(name, arguments))
    assert not result.is_error, result.content
    if result.structured_content is not None:
        return result.structured_content.get("result", result.structured_content)
    return json.loads(result.content[0].text)


def test_server_exposes_read_tools_and_propose_but_nothing_that_changes_an_order(tools):
    listed = {tool.name: tool for tool in asyncio.run(build_server(tools).list_tools())}

    assert set(listed) == {
        "list_orders",
        "get_order",
        "get_order_trades",
        "list_counterparties",
        "get_credit",
        "list_products",
        "propose_order",
        "list_proposals",
    }
    # The model chooses tools from these descriptions, so they must be there and say what matters
    assert all(tool.description for tool in listed.values())
    assert "does not place the order" in listed["propose_order"].description
    assert {"instrument", "side", "quantity", "counterparty", "delivery_period", "rationale"} <= set(
        listed["propose_order"].input_schema["required"]
    )


def test_agent_can_draft_but_the_order_only_exists_once_a_person_confirms(api, tools):
    server = build_server(tools)

    proposal = _call(server, "propose_order", {**_ORDER, "rationale": "Client asked for 50k Jan HH at 3.45"})

    assert (proposal["status"], proposal["proposed_by"]) == ("Proposed", "ai-agent")
    assert proposal["order"]["order_type"] == "Limit"
    assert tools.list_orders() == []
    assert [p["proposal_id"] for p in tools.list_proposals(status="Proposed")] == [proposal["proposal_id"]]

    # The agent's own identity cannot confirm it; a trader can
    assert api.post(f"/api/v1/proposals/{proposal['proposal_id']}/confirm").status_code == 403
    order = api.post(f"/api/v1/proposals/{proposal['proposal_id']}/confirm", headers=_TRADER).json()

    assert tools.get_order(order["order_id"])["created_by"] == "trader1"
    assert [o["order_id"] for o in tools.list_orders(status="Pending", counterparty="ACME")] == [order["order_id"]]
    assert tools.list_orders(status="Filled") == []
    assert tools.list_proposals(status="Proposed") == []


def test_proposal_without_a_price_is_a_market_order(tools):
    proposal = tools.propose_order(**{**_ORDER, "price": ""}, rationale="at market")
    assert (proposal["order"]["order_type"], proposal["order"]["price"]) == ("Market", "")


def test_agent_can_follow_an_order_through_to_its_trades(api, tools):
    order_id = api.post("/api/v1/orders", json={**_ORDER, "order_type": "Limit"}, headers=_TRADER).json()["order_id"]
    api.patch(f"/api/v1/orders/{order_id}", json={"status": "Working"}, headers=_TRADER)
    api.patch(f"/api/v1/orders/{order_id}", json={"status": "Filled"}, headers=_TRADER)

    (trade,) = tools.get_order_trades(order_id)
    assert (trade["trade_id"], trade["status"]) == (f"{order_id}-F1", "Booked")


def test_reference_data_tools(tools):
    assert "Henry Hub Natural Gas" in [p["instrument"] for p in tools.list_products()]
    assert tools.list_counterparties() == []
    assert tools.get_credit("NOLIMIT") == {"counterparty_symbol": "NOLIMIT", "note": "No credit limit is configured"}


def test_api_refusals_reach_the_agent_with_the_reason(tools):
    with pytest.raises(OmsApiError, match=r"Order ORD-NONE not found \(HTTP 404\)"):
        tools.get_order("ORD-NONE")
    with pytest.raises(OmsApiError, match="Quantity must be a number"):
        tools.propose_order(**{**_ORDER, "quantity": "lots"}, rationale="?")

    # Through the server the reason must survive: MCP withholds the text of any other exception
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError, match="Order ORD-NONE not found"):
        asyncio.run(build_server(tools).call_tool("get_order", {"order_id": "ORD-NONE"}))


def test_unreachable_api_is_reported_not_raised_raw():
    import httpx

    def _refuse(request):
        raise httpx.ConnectError("connection refused")

    with httpx.Client(base_url="http://oms.invalid", transport=httpx.MockTransport(_refuse)) as http:
        with pytest.raises(OmsApiError, match="could not be reached"):
            OmsApiTools(http).list_orders()
