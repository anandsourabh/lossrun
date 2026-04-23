"""
Unit tests for the WF Loss Run Persistence Service.
Uses unittest.mock to patch psycopg2 — no real DB required.
"""
from __future__ import annotations

import json
import uuid
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from wf_lossrun_persistence.models import (
    Address,
    Broker,
    Carrier,
    Claim,
    ClaimFinancials,
    Insured,
    Location,
    NoLossAttestation,
    Policy,
    PolicyPeriod,
    ReportMetadata,
    WFPropertyLossRunExtraction,
)
from wf_lossrun_persistence.service import LossRunPersistenceService
from wf_lossrun_persistence.exceptions import (
    CoverageTypeNotFoundError,
    DuplicateReportError,
)
from wf_lossrun_persistence import sql as SQL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_extraction(
    *,
    policy_number: str = "POL-001",
    has_claims: bool = True,
    no_loss: bool = False,
    carrier_name: str = "Marsh Carrier",
) -> WFPropertyLossRunExtraction:
    claims = []
    if has_claims:
        claims = [
            Claim(
                claimNumber="CLM-001",
                lossDate=date(2021, 2, 13),
                causeOfLoss="Freeze",
                catCode="CAT 2115",
                status="Closed",
                financials=ClaimFinancials(
                    paidLoss=915859.56,
                    reserveLoss=0.0,
                    totalIncurred=1015859.56,
                    deductibleApplied=100000.0,
                    netAdvance=915859.56,
                ),
            )
        ]

    return WFPropertyLossRunExtraction(
        reportMetadata=ReportMetadata(
            source="Sedgwick",
            documentType="Loss Run",
            valuationDate=date(2023, 5, 8),
            reportDate=date(2023, 5, 8),
        ),
        broker=Broker(
            name="Marsh USA LLC",
            contactName="Erin Sullivan",
            licenseNumber="0437153",
        ),
        insured=Insured(name="Greystar Real Estate Partners"),
        location=Location(
            name="Grapevine TwentyFour 99",
            propertyType="Multifamily Apartment",
            address=Address(
                street="3601 Grapevine Mills Pkwy",
                city="Grapevine",
                state="TX",
                zip="76051",
            ),
        ),
        policies=[
            Policy(
                policyNumber=policy_number,
                carrier=Carrier(name=carrier_name),
                coverageLine="Property",
                policyPeriod=PolicyPeriod(
                    startDate=date(2022, 1, 1),
                    endDate=date(2023, 1, 1),
                ),
                deductible=100000.0,
                claims=claims,
            )
        ],
        noLossAttestation=(
            NoLossAttestation(confirmedNoLosses=True, asOfDate=date(2023, 5, 8))
            if no_loss else None
        ),
    )


def _ids(**overrides):
    """Return a namespace of fresh UUIDs with optional overrides."""
    defaults = {k: str(uuid.uuid4()) for k in
                ("insured", "broker", "carrier", "property", "policy",
                 "report", "claim", "coverage_type")}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _cursor_with_responses(responses: list) -> MagicMock:
    """Build a mock cursor whose successive fetchone() calls consume responses."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = responses
    return cursor


def make_cursor(*, existing_report_id: str = None, **id_overrides) -> MagicMock:
    """
    Build a mock cursor for the standard happy-path 8-step sequence.
    fetchone() is called exactly once per step that returns a row:
      1  upsert_insured
      2  upsert_broker
      3  upsert_carrier
      4  upsert_property
      5  upsert_policy
         (property_policy executes but does NOT call fetchone)
      6  check_existing_report       → None or existing row
      7  insert_report               → new row  (only when no dup)
      8  select_coverage_type
      9  select_existing_claim       → None
      10 select_existing_claim_cross → None
      11 insert_claim
    """
    ids = _ids(**id_overrides)
    responses: list = [
        {"id": ids.insured},
        {"id": ids.broker},
        {"id": ids.carrier},
        {"id": ids.property},
        {"id": ids.policy},
        # step 6 — report dup check
        {"id": existing_report_id} if existing_report_id else None,
    ]
    if not existing_report_id:
        responses.append({"id": ids.report})       # step 7 — insert_report
    # steps 8-11 (claims are still processed even on idempotent skip)
    responses += [
        {"id": ids.coverage_type},                 # step 8
        None,                                      # step 9 — no same-report dup
        None,                                      # step 10 — no cross-report dup
        {"id": ids.claim},                         # step 11
    ]
    return _cursor_with_responses(responses)


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestClaimResolvedFields:
    def test_resolved_cause_combined(self):
        c = Claim(causeOfLoss="Freeze", catCode="CAT 2115")
        assert c.resolved_cause == "Freeze (CAT 2115)"

    def test_resolved_cause_no_cat(self):
        c = Claim(causeOfLoss="Fire")
        assert c.resolved_cause == "Fire"

    def test_resolved_cause_cat_only(self):
        c = Claim(catCode="CAT 2050")
        assert c.resolved_cause == "CAT 2050"

    def test_resolved_notes_merged(self):
        c = Claim(claimantName="John Doe", tpaNotes="Slip and fall near pool.")
        assert "Claimant: John Doe" in c.resolved_notes
        assert "Slip and fall" in c.resolved_notes

    def test_resolved_notes_claimant_only(self):
        c = Claim(claimantName="Jane Doe")
        assert c.resolved_notes == "Claimant: Jane Doe"


class TestClaimFinancialsResolution:
    def test_proof_of_loss_total_incurred_fallback(self):
        fin = ClaimFinancials(grossAdvance=350000.0)
        assert fin.resolved_total_incurred == 350000.0

    def test_proof_of_loss_total_paid_fallback(self):
        fin = ClaimFinancials(carrierShare=125000.0)
        assert fin.resolved_total_paid == 125000.0

    def test_standard_fields_preferred(self):
        fin = ClaimFinancials(
            totalIncurred=500.0, grossAdvance=999.0,
            paidLoss=400.0,      carrierShare=888.0,
        )
        assert fin.resolved_total_incurred == 500.0
        assert fin.resolved_total_paid == 400.0


class TestExtractionParsing:
    def test_round_trip_json(self):
        e = make_extraction()
        dumped = e.model_dump(by_alias=True)
        restored = WFPropertyLossRunExtraction.model_validate(dumped)
        assert restored.insured.name == e.insured.name

    def test_metadata_alias(self):
        raw = {
            "reportMetadata": {"source": "Marsh", "documentType": "Loss Run"},
            "insured": {"name": "Greystar"},
            "location": {"name": "Prop A", "address": {"street": "123 Main St"}},
            "policies": [],
            "_metadata": {"sourceFile": "file.pdf", "confidence": "high"},
        }
        e = WFPropertyLossRunExtraction.model_validate(raw)
        assert e.metadata.sourceFile == "file.pdf"


# ---------------------------------------------------------------------------
# Service tests (mocked DB)
# ---------------------------------------------------------------------------

def _patch_connect(cursor: MagicMock):
    """
    Return a context manager that patches psycopg2.connect and wires
    the given cursor mock through the full conn/cursor context-manager chain.
    """
    conn = MagicMock()
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    conn.autocommit = False

    # cursor() returns a context manager that yields the mock cursor
    cur_cm = MagicMock()
    cur_cm.__enter__ = lambda s: cursor
    cur_cm.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur_cm

    return patch("wf_lossrun_persistence.service.psycopg2.connect", return_value=conn)


class TestLossRunPersistenceService:

    def _make_service(self, idempotent: bool = True) -> LossRunPersistenceService:
        return LossRunPersistenceService(dsn="postgresql://test/test", idempotent=idempotent)

    def test_happy_path_returns_result(self):
        fixed_report_id = str(uuid.uuid4())
        fixed_claim_id  = str(uuid.uuid4())
        cursor = make_cursor(report_id=fixed_report_id, claim_id=fixed_claim_id)

        with _patch_connect(cursor):
            svc = self._make_service()
            result = svc.persist(make_extraction(), source_file="test.pdf")

        assert result.report_id is not None
        assert len(result.claim_ids) == 1
        assert not result.skipped_duplicate_report
        assert not result.no_loss_document

    def test_no_loss_skips_claim_insertion(self):
        ids = _ids()
        # No claims: steps 8-11 never execute
        responses = [
            {"id": ids.insured}, {"id": ids.broker}, {"id": ids.carrier},
            {"id": ids.property}, {"id": ids.policy},
            None,                  # check_existing_report → not found
            {"id": ids.report},    # insert_report
        ]
        cursor = _cursor_with_responses(responses)
        with _patch_connect(cursor):
            svc = self._make_service()
            result = svc.persist(
                make_extraction(no_loss=True, has_claims=False),
                source_file="no_loss.pdf",
            )
        assert result.no_loss_document is True
        assert len(result.claim_ids) == 0

    def test_idempotent_mode_skips_duplicate_report(self):
        dup_report_id = str(uuid.uuid4())
        cursor = make_cursor(existing_report_id=dup_report_id)
        with _patch_connect(cursor):
            svc = self._make_service(idempotent=True)
            result = svc.persist(make_extraction(), source_file="test.pdf")
        assert result.skipped_duplicate_report is True

    def test_strict_mode_raises_on_duplicate(self):
        dup_report_id = str(uuid.uuid4())
        cursor = make_cursor(existing_report_id=dup_report_id)
        with _patch_connect(cursor):
            svc = self._make_service(idempotent=False)
            with pytest.raises(DuplicateReportError):
                svc.persist(make_extraction(), source_file="test.pdf")

    def test_coverage_line_aliases(self):
        assert SQL.COVERAGE_LINE_ALIASES["AOP"] == "Property"
        assert SQL.COVERAGE_LINE_ALIASES["All Risk"] == "Property"
        assert SQL.COVERAGE_LINE_ALIASES["Casualty"] == "General Liability"
        assert SQL.COVERAGE_LINE_ALIASES["Property"] == "Property"


# ---------------------------------------------------------------------------
# SQL constants sanity checks
# ---------------------------------------------------------------------------

class TestSqlConstants:
    def test_no_format_string_injection(self):
        """All SQL must use %(name)s placeholders, never f-strings or %s."""
        stmts = [
            SQL.UPSERT_INSURED, SQL.UPSERT_BROKER, SQL.UPSERT_CARRIER,
            SQL.UPSERT_PROPERTY, SQL.UPSERT_POLICY_WITH_NUMBER,
            SQL.INSERT_REPORT, SQL.INSERT_CLAIM, SQL.UPDATE_CLAIM_FINANCIALS,
        ]
        for stmt in stmts:
            # Bare %s (without a name) should not appear
            import re
            bare = re.findall(r"%(?!\()(?!%)", stmt)
            assert not bare, f"Bare %s found in SQL:\n{stmt}"
