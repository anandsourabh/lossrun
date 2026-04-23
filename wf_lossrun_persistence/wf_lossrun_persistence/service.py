"""
WF Property Loss Run - Persistence Service (flat schema)

Persists a WFPropertyLossRunExtraction into the flat wf_property_lossrun_*
tables. The upsert sequence:

    1. Insured      (upsert on name)
    2. Broker       (upsert on name, optional)
    3. Property     (upsert on name+address)
    4. Carriers     (upsert on name, dedup across policies)
    5. Report       (check source_file+valuation_date; insert if new)
    6. Claims       (flat rows; policy + coverage fields inline)

Usage:
    from wf_lossrun_persistence.service import LossRunPersistenceService
    from wf_lossrun_persistence.models import WFPropertyLossRunExtraction

    svc = LossRunPersistenceService(dsn="postgresql://user:pass@host/db")
    result = svc.persist(extraction, source_file="my_file.pdf")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import psycopg2
import psycopg2.extras

from .exceptions import DuplicateReportError
from .models import (
    Broker,
    Carrier,
    Claim,
    ClaimFinancials,
    Insured,
    Location,
    Policy,
    WFPropertyLossRunExtraction,
)
from . import sql as SQL

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result DTO
# ---------------------------------------------------------------------------

@dataclass
class PersistenceResult:
    """IDs of every row created or resolved during a single persist() call."""
    insured_id: Optional[str] = None
    broker_id: Optional[str] = None
    property_id: Optional[str] = None
    carrier_ids: list[str] = field(default_factory=list)
    report_id: Optional[str] = None
    claim_ids: list[str] = field(default_factory=list)
    updated_claim_ids: list[str] = field(default_factory=list)
    skipped_duplicate_report: bool = False
    no_loss_document: bool = False

    @property
    def summary(self) -> str:
        lines = [
            f"  insured_id    : {self.insured_id}",
            f"  broker_id     : {self.broker_id}",
            f"  property_id   : {self.property_id}",
            f"  carrier_ids   : {self.carrier_ids}",
            f"  report_id     : {self.report_id}",
            f"  claims new    : {len(self.claim_ids)}",
            f"  claims updated: {len(self.updated_claim_ids)}",
            f"  no-loss doc   : {self.no_loss_document}",
            f"  dup skipped   : {self.skipped_duplicate_report}",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class LossRunPersistenceService:
    """
    Persists a parsed WFPropertyLossRunExtraction to PostgreSQL.

    Args:
        dsn: libpq connection string or DSN dict accepted by psycopg2.connect().
        idempotent: When True (default), re-processing the same source_file +
                    valuation_date skips the report insert and updates existing
                    claim financials in place. When False, a DuplicateReportError
                    is raised instead.
    """

    def __init__(self, dsn: str, *, idempotent: bool = True):
        self.dsn = dsn
        self.idempotent = idempotent

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def persist(
        self,
        extraction: WFPropertyLossRunExtraction,
        source_file: Optional[str] = None,
    ) -> PersistenceResult:
        """
        Persist one extraction payload inside a single transaction.
        The entire operation is rolled back if any step fails.
        """
        resolved_source_file = (
            source_file
            or (extraction.metadata.sourceFile if extraction.metadata else None)
        )

        with psycopg2.connect(self.dsn) as conn:
            conn.autocommit = False
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    result = self._run(cur, extraction, resolved_source_file)
                    conn.commit()
                    logger.info(
                        "Persisted '%s' -> report_id=%s  claims_new=%d  claims_updated=%d",
                        resolved_source_file,
                        result.report_id,
                        len(result.claim_ids),
                        len(result.updated_claim_ids),
                    )
                    return result
                except Exception:
                    conn.rollback()
                    raise

    def ensure_constraints(self, dsn: Optional[str] = None) -> None:
        """Create unique indexes required by ON CONFLICT. Idempotent."""
        with psycopg2.connect(dsn or self.dsn) as conn:
            with conn.cursor() as cur:
                for stmt in SQL.REQUIRED_UNIQUE_CONSTRAINTS.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)
            conn.commit()
        logger.info("Unique constraint indexes verified/created.")

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def _run(
        self,
        cur: psycopg2.extensions.cursor,
        e: WFPropertyLossRunExtraction,
        source_file: Optional[str],
    ) -> PersistenceResult:
        result = PersistenceResult()

        # 1. Insured
        result.insured_id = self._upsert_insured(cur, e.insured)

        # 2. Broker (optional)
        if e.broker and e.broker.name:
            result.broker_id = self._upsert_broker(cur, e.broker)

        # 3. Property
        result.property_id = self._upsert_property(cur, e.location)

        # 4. Carriers (dedup across policies)
        carrier_id_by_name: dict[str, str] = {}
        for policy in e.policies:
            if policy.carrier and policy.carrier.name:
                name = policy.carrier.name
                if name not in carrier_id_by_name:
                    cid = self._upsert_carrier(cur, policy.carrier)
                    carrier_id_by_name[name] = cid
                    result.carrier_ids.append(cid)

        # 5. Report
        existing_report_id = self._check_existing_report(
            cur, source_file, e.reportMetadata.valuationDate
        )
        if existing_report_id:
            if not self.idempotent:
                raise DuplicateReportError(existing_report_id, source_file or "")
            result.report_id = existing_report_id
            result.skipped_duplicate_report = True
            logger.info(
                "Idempotent mode: report exists (id=%s); updating claims only.",
                existing_report_id,
            )
        else:
            result.report_id = self._insert_report(
                cur, e, result.property_id, result.insured_id,
                result.broker_id, source_file,
            )

        # 6. Claims (skipped when this is a no-loss attestation)
        is_no_loss = bool(
            e.noLossAttestation and e.noLossAttestation.confirmedNoLosses
        )
        result.no_loss_document = is_no_loss
        if is_no_loss:
            logger.info(
                "No-loss attestation — skipping claim inserts for report %s",
                result.report_id,
            )
            return result

        for policy in e.policies:
            carrier_id = (
                carrier_id_by_name.get(policy.carrier.name)
                if policy.carrier and policy.carrier.name
                else None
            )
            coverage_line = SQL.COVERAGE_LINE_ALIASES.get(
                policy.coverageLine, policy.coverageLine
            )
            for claim in policy.claims:
                claim_id, was_updated = self._upsert_claim(
                    cur, claim, policy, result.report_id,
                    result.property_id, carrier_id, coverage_line,
                )
                if was_updated:
                    result.updated_claim_ids.append(claim_id)
                else:
                    result.claim_ids.append(claim_id)

        return result

    # ------------------------------------------------------------------
    # Step implementations
    # ------------------------------------------------------------------

    def _upsert_insured(self, cur, insured: Insured) -> str:
        cur.execute(SQL.UPSERT_INSURED, {
            "name":         insured.name,
            "contact_name": insured.portfolioOwner,
        })
        return str(cur.fetchone()["id"])

    def _upsert_broker(self, cur, broker: Broker) -> str:
        cur.execute(SQL.UPSERT_BROKER, {
            "name":           broker.name,
            "contact_name":   broker.contactName,
            "phone":          broker.phone,
            "email":          broker.email,
            "license_number": broker.licenseNumber,
            "address":        broker.address,
        })
        return str(cur.fetchone()["id"])

    def _upsert_carrier(self, cur, carrier: Carrier) -> str:
        cur.execute(SQL.UPSERT_CARRIER, {
            "name":      carrier.name,
            "naic_code": carrier.naicCode,
        })
        return str(cur.fetchone()["id"])

    def _upsert_property(self, cur, location: Location) -> str:
        cur.execute(SQL.UPSERT_PROPERTY, {
            "name":          location.name,
            "address":       location.address.street,
            "city":          location.address.city,
            "state":         location.address.state,
            "zip":           location.address.zip,
            "property_type": location.propertyType,
        })
        return str(cur.fetchone()["id"])

    def _check_existing_report(
        self,
        cur,
        source_file: Optional[str],
        valuation_date,
    ) -> Optional[str]:
        if not source_file:
            return None
        cur.execute(SQL.CHECK_EXISTING_REPORT, {
            "source_file":    source_file,
            "valuation_date": valuation_date,
        })
        row = cur.fetchone()
        return str(row["id"]) if row else None

    def _insert_report(
        self,
        cur,
        e: WFPropertyLossRunExtraction,
        property_id: str,
        insured_id: str,
        broker_id: Optional[str],
        source_file: Optional[str],
    ) -> str:
        meta = e.reportMetadata
        period_start = period_end = None
        if meta.lookbackPeriod:
            period_start = meta.lookbackPeriod.startDate
            period_end   = meta.lookbackPeriod.endDate

        no_loss = e.noLossAttestation
        # For no-loss letters the attestation date can fill in valuation_date
        valuation_date = meta.valuationDate
        if not valuation_date and no_loss:
            valuation_date = no_loss.asOfDate

        cur.execute(SQL.INSERT_REPORT, {
            "property_id":        property_id,
            "insured_id":         insured_id,
            "broker_id":          broker_id,
            "valuation_date":     valuation_date,
            "report_date":        meta.reportDate,
            "period_start":       period_start,
            "period_end":         period_end,
            "document_type":      meta.documentType,
            "source_file":        source_file,
            "source_system":      meta.source,
            "no_loss_confirmed":  bool(no_loss and no_loss.confirmedNoLosses),
            "no_loss_as_of_date": no_loss.asOfDate if no_loss else None,
        })
        return str(cur.fetchone()["id"])

    def _upsert_claim(
        self,
        cur,
        claim: Claim,
        policy: Policy,
        report_id: str,
        property_id: str,
        carrier_id: Optional[str],
        coverage_line: str,
    ) -> tuple[str, bool]:
        """
        Returns (claim_id, was_updated).

        Strategy:
            1. Check for existing claim in THIS report (skip if already there).
            2. Else check across reports for this property by claim_number/DOL
               -> update financials in place and repoint report_id.
            3. Else fresh insert.
        """
        fin: Optional[ClaimFinancials] = claim.financials
        period = policy.policyPeriod

        params = {
            "report_id":                report_id,
            "carrier_id":               carrier_id,
            "policy_number":            policy.policyNumber,
            "policy_effective_date":    period.startDate if period else None,
            "policy_expiration_date":   period.endDate   if period else None,
            "policy_limit":             policy.policyLimit,
            "policy_deductible_amount": policy.deductible,
            "policy_deductible_type":   policy.deductibleType,
            "coverage_line":            coverage_line,
            "claim_number":             claim.claimNumber,
            "date_of_loss":             claim.lossDate,
            "cause_of_loss":            claim.resolved_cause,
            "loss_description":         claim.description,
            "status":                   claim.status,
            "total_incurred":           fin.resolved_total_incurred if fin else None,
            "total_paid":               fin.resolved_total_paid     if fin else None,
            "outstanding_reserve":      fin.reserveLoss             if fin else None,
            "lae_reserve":              fin.reserveLAE              if fin else None,
            "lae_paid":                 fin.paidLAE                 if fin else None,
            "deductible_applied":       fin.deductibleApplied       if fin else None,
            "amount_less_deductible":   fin.netAdvance              if fin else None,
            "notes":                    claim.resolved_notes,
        }

        # 1. Same-report dedup
        cur.execute(SQL.SELECT_EXISTING_CLAIM, {
            "report_id":    report_id,
            "claim_number": claim.claimNumber,
            "date_of_loss": claim.lossDate,
        })
        row = cur.fetchone()
        if row:
            logger.debug(
                "Claim %s already in report %s - skipping.",
                claim.claimNumber, report_id,
            )
            return str(row["id"]), False

        # 2. Cross-report dedup
        if claim.claimNumber:
            cur.execute(SQL.SELECT_EXISTING_CLAIM_CROSS_REPORT, {
                "property_id":  property_id,
                "claim_number": claim.claimNumber,
                "date_of_loss": claim.lossDate,
            })
            row = cur.fetchone()
            if row:
                cur.execute(SQL.UPDATE_CLAIM_FINANCIALS, {**params, "id": str(row["id"])})
                updated = cur.fetchone()
                logger.debug(
                    "Updated existing claim %s (id=%s) with latest financials.",
                    claim.claimNumber, row["id"],
                )
                return str(updated["id"]), True

        # 3. Fresh insert
        cur.execute(SQL.INSERT_CLAIM, params)
        return str(cur.fetchone()["id"]), False
