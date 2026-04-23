"""
Pydantic models for the WFPropertyLossRunExtraction JSON schema.
These mirror the extraction schema exactly — no DB concerns here.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class Address(BaseModel):
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None


class FmGlobal(BaseModel):
    indexNumber: Optional[str] = None
    locationNumber: Optional[str] = None


class Location(BaseModel):
    name: Optional[str] = None
    propertyType: Optional[str] = None
    address: Address
    fmGlobal: Optional[FmGlobal] = None


class Insured(BaseModel):
    name: str
    portfolioOwner: Optional[str] = None


class Broker(BaseModel):
    name: Optional[str] = None
    contactName: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    licenseNumber: Optional[str] = None
    address: Optional[str] = None


class Carrier(BaseModel):
    name: Optional[str] = None
    naicCode: Optional[str] = None


class PolicyPeriod(BaseModel):
    startDate: Optional[date] = None
    endDate: Optional[date] = None


class ClaimFinancials(BaseModel):
    paidLoss: Optional[float] = None
    paidLAE: Optional[float] = None
    reserveLoss: Optional[float] = None
    reserveLAE: Optional[float] = None
    totalIncurred: Optional[float] = None
    grossAdvance: Optional[float] = None
    deductibleApplied: Optional[float] = None
    netAdvance: Optional[float] = None
    carrierShare: Optional[float] = None

    @property
    def resolved_total_incurred(self) -> Optional[float]:
        """Fall back to grossAdvance when totalIncurred is absent (Proof of Loss format)."""
        return self.totalIncurred if self.totalIncurred is not None else self.grossAdvance

    @property
    def resolved_total_paid(self) -> Optional[float]:
        """Fall back to carrierShare when paidLoss is absent (Proof of Loss format)."""
        return self.paidLoss if self.paidLoss is not None else self.carrierShare


class Claim(BaseModel):
    claimNumber: Optional[str] = None
    lossDate: Optional[date] = None
    causeOfLoss: Optional[str] = None
    catCode: Optional[str] = None
    description: Optional[str] = None
    status: Optional[Literal["Open", "Closed", "Withdrawn", "Below Deductible", "Unknown"]] = None
    claimantName: Optional[str] = None
    financials: Optional[ClaimFinancials] = None
    tpaNotes: Optional[str] = None

    @property
    def resolved_cause(self) -> Optional[str]:
        """Combine causeOfLoss and catCode into a single string."""
        if self.causeOfLoss and self.catCode:
            return f"{self.causeOfLoss} ({self.catCode})"
        return self.causeOfLoss or self.catCode

    @property
    def resolved_notes(self) -> Optional[str]:
        """Merge claimantName prefix into tpaNotes."""
        parts = []
        if self.claimantName:
            parts.append(f"Claimant: {self.claimantName}")
        if self.tpaNotes:
            parts.append(self.tpaNotes)
        return "\n".join(parts) if parts else None


class Policy(BaseModel):
    policyNumber: Optional[str] = None
    carrier: Optional[Carrier] = None
    coverageLine: str
    policyPeriod: Optional[PolicyPeriod] = None
    policyLimit: Optional[float] = None
    deductible: Optional[float] = None
    deductibleType: Optional[str] = None
    claims: List[Claim] = Field(default_factory=list)


class LookbackPeriod(BaseModel):
    startDate: Optional[date] = None
    endDate: Optional[date] = None


class ReportMetadata(BaseModel):
    source: Optional[str] = None
    documentType: Optional[str] = None
    valuationDate: Optional[date] = None
    reportDate: Optional[date] = None
    lookbackPeriod: Optional[LookbackPeriod] = None


class NoLossAttestation(BaseModel):
    confirmedNoLosses: Optional[bool] = None
    asOfDate: Optional[date] = None
    lookbackPeriod: Optional[LookbackPeriod] = None
    statementSource: Optional[str] = None


class ExtractionMetadata(BaseModel):
    sourceFile: Optional[str] = None
    fileId: Optional[str] = None
    extractionDate: Optional[datetime] = None
    extractionVersion: Optional[str] = None
    confidence: Optional[Literal["high", "medium", "low"]] = None
    parsingNotes: Optional[str] = None


class WFPropertyLossRunExtraction(BaseModel):
    reportMetadata: ReportMetadata
    broker: Optional[Broker] = None
    insured: Insured
    location: Location
    policies: List[Policy] = Field(default_factory=list)
    noLossAttestation: Optional[NoLossAttestation] = None
    metadata: Optional[ExtractionMetadata] = Field(default=None, alias="_metadata")

    model_config = {"populate_by_name": True}
