# WF Property Loss Run — Persistence Mapping

## Architecture

```
PDF / XLSX / DOCX
       │
       ▼
  LLM Extraction
  (uses extraction schema)
       │
       ▼
  Structured JSON
  (WFPropertyLossRunExtraction)
       │
       ▼
  Persistence Service
  (resolves natural keys → UUIDs,
   upserts reference tables,
   inserts transactional rows)
       │
       ▼
  wf_property_lossrun_* tables
```

---

## Upsert Order (strict — respects FK dependencies)

Run in this exact sequence per document. Steps 1–5 are reference
entities (upsert by natural key). Steps 6–9 are transactional rows
(insert only, no upsert).

| Step | Table                              | Source JSON path         | Natural Key (upsert on)        |
|------|------------------------------------|--------------------------|--------------------------------|
| 1    | wf_property_lossrun_insured        | insured                  | name                           |
| 2    | wf_property_lossrun_broker         | broker                   | name                           |
| 3    | wf_property_lossrun_carrier        | policies[].carrier       | name                           |
| 4    | wf_property_lossrun_property       | location                 | name + address.street          |
| 5    | wf_property_lossrun_policy         | policies[]               | policyNumber + effectiveDate   |
| 6    | wf_property_lossrun_property_policy| policies[] × location    | property_id + policy_id        |
| 7    | wf_property_lossrun_report         | reportMetadata + broker  | (insert; source_file + valuation_date for idempotency) |
| 8    | wf_property_lossrun_claim          | policies[].claims[]      | insert (child of report row)   |

`wf_property_lossrun_coverage_type` is pre-seeded and resolved by
label match only — never inserted by the service.

---

## Field-to-Column Mapping

### wf_property_lossrun_insured
| JSON path              | Column        | Notes                        |
|------------------------|---------------|------------------------------|
| insured.name           | name          | Upsert key                   |
| insured.portfolioOwner | contact_name  | Store as secondary identifier|

### wf_property_lossrun_broker
| JSON path                | Column         | Notes      |
|--------------------------|----------------|------------|
| broker.name              | name           | Upsert key |
| broker.contactName       | contact_name   |            |
| broker.phone             | phone          |            |
| broker.email             | email          |            |
| broker.licenseNumber     | license_number |            |
| broker.address           | address        |            |

### wf_property_lossrun_carrier
| JSON path               | Column    | Notes      |
|-------------------------|-----------|------------|
| policies[].carrier.name | name      | Upsert key |
| policies[].carrier.naicCode | naic_code |         |

### wf_property_lossrun_property
| JSON path              | Column        | Notes                  |
|------------------------|---------------|------------------------|
| location.name          | name          | Upsert key (part 1)    |
| location.address.street| address       | Upsert key (part 2)    |
| location.address.city  | city          |                        |
| location.address.state | state         |                        |
| location.address.zip   | zip           |                        |
| location.propertyType  | property_type |                        |

### wf_property_lossrun_policy
| JSON path                      | Column             | Notes                       |
|--------------------------------|--------------------|-----------------------------|
| policies[].policyNumber        | policy_number      | Upsert key (part 1)         |
| policies[].policyPeriod.startDate | effective_date  | Upsert key (part 2)         |
| policies[].policyPeriod.endDate   | expiration_date |                             |
| policies[].policyLimit         | total_limit        |                             |
| policies[].deductible          | deductible_amount  |                             |
| policies[].deductibleType      | deductible_type    |                             |
| → resolved insured.id          | insured_id         | FK from step 1              |
| → resolved carrier.id          | carrier_id         | FK from step 3              |

### wf_property_lossrun_report
| JSON path                          | Column          | Notes                                 |
|------------------------------------|-----------------|---------------------------------------|
| reportMetadata.source              | source_system   |                                       |
| reportMetadata.documentType        | document_type   |                                       |
| reportMetadata.valuationDate       | valuation_date  |                                       |
| reportMetadata.reportDate          | report_date     |                                       |
| reportMetadata.lookbackPeriod.startDate | period_start |                                      |
| reportMetadata.lookbackPeriod.endDate   | period_end   |                                      |
| _metadata.sourceFile               | source_file     |                                       |
| → resolved property.id             | property_id     | FK from step 4                        |
| → resolved policy.id               | policy_id       | FK from step 5 (first policy, or null)|
| → resolved broker.id               | broker_id       | FK from step 2                        |

### wf_property_lossrun_claim
| JSON path                                    | Column                 | Notes                                            |
|----------------------------------------------|------------------------|--------------------------------------------------|
| policies[].claims[].claimNumber              | claim_number           |                                                  |
| policies[].claims[].lossDate                 | date_of_loss           |                                                  |
| policies[].claims[].causeOfLoss              | cause_of_loss          | Append catCode if present: "Freeze (CAT 2115)"   |
| policies[].claims[].description              | loss_description       |                                                  |
| policies[].claims[].status                   | status                 |                                                  |
| policies[].claims[].financials.paidLoss      | total_paid             |                                                  |
| policies[].claims[].financials.paidLAE       | lae_paid               |                                                  |
| policies[].claims[].financials.reserveLoss   | outstanding_reserve    |                                                  |
| policies[].claims[].financials.reserveLAE    | lae_reserve            |                                                  |
| policies[].claims[].financials.totalIncurred | total_incurred         | Fall back to grossAdvance if null                |
| policies[].claims[].financials.deductibleApplied | deductible_applied |                                                  |
| policies[].claims[].financials.netAdvance    | amount_less_deductible |                                                  |
| policies[].claims[].financials.carrierShare  | total_paid             | Use when paidLoss is null (Proof of Loss format) |
| policies[].claims[].tpaNotes                 | notes                  | Prepend claimantName if present                  |
| → resolved report.id                         | loss_run_report_id     | FK from step 7                                   |
| → resolved coverage_type.id by label match   | coverage_type_id       | Match policies[].coverageLine to coverage_type.label |

---

## Edge Case Handling

### Multiple policies in one document
Some documents review both Property and GL under the same report (e.g.,
Lockton, Marsh). Each `policies[]` item produces one policy row. All
policies from the same document share the same `loss_run_report_id`.

### No-loss documents
When `noLossAttestation.confirmedNoLosses = true`, insert the report row
normally but skip claim insertion. Do not insert a placeholder claim row.

### Missing policyNumber
Some documents (loss letters, no-loss letters) do not include a policy
number. Insert the policy row with `policy_number = NULL` and use
`(insured_id, carrier_id, effective_date)` as the functional upsert key.

### Duplicate claim detection
Before inserting a claim, check for an existing row matching
`(claim_number, date_of_loss)` tied to the same property. If found,
update financial figures rather than inserting a duplicate. This handles
the case where the same loss appears in both an older and a newer run
with revised reserves.

### Proof of Loss financials
These documents report `grossAdvance`, `deductibleApplied`, and
`netAdvance` / `carrierShare` rather than the standard
paid/reserve/incurred columns. Map as:
  - grossAdvance   → total_incurred
  - netAdvance     → amount_less_deductible
  - carrierShare   → total_paid
  - deductibleApplied → deductible_applied

### FM Global fmGlobal fields
`location.fmGlobal.indexNumber` and `location.fmGlobal.locationNumber`
have no current column in wf_property_lossrun_property. Store in a
`notes` column or add dedicated columns when FM Global volume warrants it.

---

## Idempotency Key (re-processing safety)

To safely re-run extraction on the same document:
  - Check for existing report row matching `source_file + valuation_date`
  - If found, skip report insert and use existing `report_id` for claims
  - Upsert claims by `(loss_run_report_id, claim_number, date_of_loss)`
