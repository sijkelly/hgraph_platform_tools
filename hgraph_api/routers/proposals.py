"""Order proposal endpoints: an agent drafts an order, a person confirms or dismisses it.

Creating a proposal changes nothing in the order book.  Confirming one submits the order as
the confirming user, through the same checks as any other order, and the person confirming
must not be the one who drafted it.
"""

import logging
import os

from fastapi import APIRouter, HTTPException, Request

from hgraph_api.routers.orders import order_details_from_request, submit_new_order
from hgraph_api.schemas.orders import (
    CreateOrderRequest,
    OrderResponse,
    ProposalDecisionRequest,
    ProposalResponse,
    ProposeOrderRequest,
)
from hgraph_api.services.order_engine import new_order_id
from hgraph_oms.proposals import CONFIRMED, DISMISSED, PROPOSED, OrderProposal, ProposalStore

__all__ = ("router", "PROPOSALS_DB")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/proposals", tags=["proposals"])

PROPOSALS_DB = "order_proposals.db"


def _store(request: Request) -> ProposalStore:
    return ProposalStore(os.path.join(getattr(request.app.state, "db_dir", "."), PROPOSALS_DB))


def _user_id(request: Request) -> str:
    return getattr(request.state, "user_id", "")


def _response(proposal: OrderProposal) -> ProposalResponse:
    return ProposalResponse(
        proposal_id=proposal.proposal_id,
        status=proposal.status,
        order=CreateOrderRequest(**proposal.fields),
        proposed_by=proposal.proposed_by,
        source_text=proposal.source_text,
        rationale=proposal.rationale,
        created_at=proposal.created_at,
        decided_by=proposal.decided_by,
        decided_at=proposal.decided_at,
        order_id=proposal.order_id,
        reason=proposal.reason,
    )


def _open_proposal(store: ProposalStore, proposal_id: str) -> OrderProposal:
    proposal = store.get(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail=f"Proposal {proposal_id} not found")
    if proposal.status != PROPOSED:
        raise HTTPException(status_code=409, detail=f"Proposal {proposal_id} is already {proposal.status.lower()}")
    return proposal


@router.post("", response_model=ProposalResponse, status_code=201)
def propose_order(request_body: ProposeOrderRequest, request: Request) -> ProposalResponse:
    """Draft an order for a person to confirm. A malformed order is refused with 400."""
    order_details_from_request(request_body)  # refuse what could never be submitted
    fields = request_body.model_dump(include=set(CreateOrderRequest.model_fields))
    proposal = _store(request).create(
        fields,
        proposed_by=_user_id(request),
        source_text=request_body.source_text,
        rationale=request_body.rationale,
    )
    logger.info("Order proposal %s drafted by %s", proposal.proposal_id, proposal.proposed_by)
    return _response(proposal)


@router.get("", response_model=list[ProposalResponse])
def list_proposals(request: Request, status: str | None = None) -> list[ProposalResponse]:
    """List proposals, newest first. ``status`` filters to ``Proposed``, ``Confirmed`` or ``Dismissed``."""
    return [_response(p) for p in _store(request).proposals(status=status)]


@router.get("/{proposal_id}", response_model=ProposalResponse)
def get_proposal(proposal_id: str, request: Request) -> ProposalResponse:
    proposal = _store(request).get(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail=f"Proposal {proposal_id} not found")
    return _response(proposal)


@router.post("/{proposal_id}/confirm", response_model=OrderResponse, status_code=201)
def confirm_proposal(proposal_id: str, request: Request) -> OrderResponse:
    """Submit the proposed order as the confirming user.

    Refused with 403 if the confirming user drafted the proposal.  If a pre-trade check refuses
    the order, the proposal stays confirmed and points at the ``Rejected`` order.
    """
    store = _store(request)
    proposal = _open_proposal(store, proposal_id)
    user_id = _user_id(request)
    if user_id == proposal.proposed_by:
        raise HTTPException(status_code=403, detail="A proposal must be confirmed by someone other than its author")

    details = order_details_from_request(CreateOrderRequest(**proposal.fields))
    order_id = new_order_id()
    # Claim the proposal first so two people confirming at once cannot both submit an order
    if store.decide(proposal_id, CONFIRMED, decided_by=user_id, order_id=order_id) is None:
        raise HTTPException(status_code=409, detail=f"Proposal {proposal_id} has already been decided")
    try:
        return submit_new_order(request, details, order_id=order_id)
    except HTTPException as exc:
        if exc.status_code == 503:
            store.reopen(proposal_id)  # nothing was submitted; let it be confirmed again later
        raise


@router.post("/{proposal_id}/dismiss", response_model=ProposalResponse)
def dismiss_proposal(proposal_id: str, request: Request, request_body: ProposalDecisionRequest | None = None):
    """Dismiss a proposal without submitting anything."""
    store = _store(request)
    _open_proposal(store, proposal_id)
    reason = request_body.reason if request_body else ""
    decided = store.decide(proposal_id, DISMISSED, decided_by=_user_id(request), reason=reason)
    if decided is None:
        raise HTTPException(status_code=409, detail=f"Proposal {proposal_id} has already been decided")
    return _response(decided)
