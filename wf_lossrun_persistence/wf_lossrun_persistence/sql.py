"""
SQL constants for the WF Loss Run persistence service.

All statements use %(name)s placeholders (psycopg2 style).

Persistence order (flat schema):
    1. Insured      (upsert on name)
    2. Broker       (upsert on name, optional)
    3. Property     (upsert on name+address)
    4. Carriers     (upsert on name, dedup across claims)
    5. Report       (insert; dedup on source_file+valuation_date for idempotency)
    6. Claims       (flat rows; dedup within report, update cross-report)
"""

# ---------------------------------------------------------------------------
# 1. Insured
# ---------------------------------------------------------------------------

UPSERT_INSURED = """
INSERT INTO wf_property_lossrun_insured (id, name, contact_name, created_at, updated_at)
VALUES (gen_random_uuid(), %(name)s, %(contact_name)s, now(), now())
ON CONFLICT (name) DO UPDATE
    SET contact_name = COALESCE(EXCLUDED.contact_name, wf_property_lossrun_insured.contact_name),
        updated_at   = now()
RETURNING id;
"""

# ---------------------------------------------------------------------------
# 2. Broker
# ---------------------------------------------------------------------------

UPSERT_BROKER = """
INSERT INTO wf_property_lossrun_broker
    (id, name, contact_name, phone, email, license_number, address, created_at, updated_at)
VALUES
    (gen_random_uuid(), %(name)s, %(contact_name)s, %(phone)s,
     %(email)s, %(license_number)s, %(address)s, now(), now())
ON CONFLICT (name) DO UPDATE
    SET contact_name   = COALESCE(EXCLUDED.contact_name,   wf_property_lossrun_broker.contact_name),
        phone          = COALESCE(EXCLUDED.phone,          wf_property_lossrun_broker.phone),
        email          = COALESCE(EXCLUDED.email,          wf_property_lossrun_broker.email),
        license_number = COALESCE(EXCLUDED.license_number, wf_property_lossrun_broker.license_number),
        address        = COALESCE(EXCLUDED.address,        wf_property_lossrun_broker.address),
        updated_at     = now()
RETURNING id;
"""

# ---------------------------------------------------------------------------
# 3. Property
# ---------------------------------------------------------------------------

UPSERT_PROPERTY = """
INSERT INTO wf_property_lossrun_property
    (id, name, address, city, state, zip, property_type, created_at, updated_at)
VALUES
    (gen_random_uuid(), %(name)s, %(address)s, %(city)s,
     %(state)s, %(zip)s, %(property_type)s, now(), now())
ON CONFLICT (name, address) DO UPDATE
    SET city          = COALESCE(EXCLUDED.city,          wf_property_lossrun_property.city),
        state         = COALESCE(EXCLUDED.state,         wf_property_lossrun_property.state),
        zip           = COALESCE(EXCLUDED.zip,           wf_property_lossrun_property.zip),
        property_type = COALESCE(EXCLUDED.property_type, wf_property_lossrun_property.property_type),
        updated_at    = now()
RETURNING id;
"""

# ---------------------------------------------------------------------------
# 4. Carrier
# ---------------------------------------------------------------------------

UPSERT_CARRIER = """
INSERT INTO wf_property_lossrun_carrier (id, name, naic_code, created_at, updated_at)
VALUES (gen_random_uuid(), %(name)s, %(naic_code)s, now(), now())
ON CONFLICT (name) DO UPDATE
    SET naic_code  = COALESCE(EXCLUDED.naic_code, wf_property_lossrun_carrier.naic_code),
        updated_at = now()
RETURNING id;
"""

# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------

CHECK_EXISTING_REPORT = """
SELECT id FROM wf_property_lossrun_report
WHERE source_file    = %(source_file)s
  AND valuation_date IS NOT DISTINCT FROM %(valuation_date)s
LIMIT 1;
"""

INSERT_REPORT = """
INSERT INTO wf_property_lossrun_report
    (id, property_id, insured_id, broker_id,
     valuation_date, report_date, period_start, period_end,
     document_type, source_file, source_system,
     no_loss_confirmed, no_loss_as_of_date,
     created_at, updated_at)
VALUES
    (gen_random_uuid(), %(property_id)s, %(insured_id)s, %(broker_id)s,
     %(valuation_date)s, %(report_date)s, %(period_start)s, %(period_end)s,
     %(document_type)s, %(source_file)s, %(source_system)s,
     %(no_loss_confirmed)s, %(no_loss_as_of_date)s,
     now(), now())
RETURNING id;
"""

# ---------------------------------------------------------------------------
# 6. Claim
# ---------------------------------------------------------------------------

# Map raw extraction coverage-line values to the three canonical labels
# accepted by the CHECK constraint on wf_property_lossrun_claim.coverage_line.
COVERAGE_LINE_ALIASES: dict[str, str] = {
    "Property":                     "Property",
    "General Liability":            "General Liability",
    "Commercial General Liability": "Commercial General Liability",
    "Casualty":                     "General Liability",
    "All Risk":                     "Property",
    "AOP":                          "Property",
    "Other":                        "Property",
}

# Same-report dedup (don't insert a claim that's already in this report)
SELECT_EXISTING_CLAIM = """
SELECT id FROM wf_property_lossrun_claim
WHERE report_id    = %(report_id)s
  AND claim_number IS NOT DISTINCT FROM %(claim_number)s
  AND date_of_loss IS NOT DISTINCT FROM %(date_of_loss)s
LIMIT 1;
"""

# Cross-report dedup: same claim re-appearing in a newer run for the same
# property. Returns the most-recent prior occurrence so we can update it
# in place and repoint it to the new report.
SELECT_EXISTING_CLAIM_CROSS_REPORT = """
SELECT c.id
FROM   wf_property_lossrun_claim c
JOIN   wf_property_lossrun_report r ON r.id = c.report_id
WHERE  r.property_id = %(property_id)s
  AND  c.claim_number = %(claim_number)s
  AND  c.date_of_loss IS NOT DISTINCT FROM %(date_of_loss)s
ORDER  BY r.valuation_date DESC NULLS LAST
LIMIT  1;
"""

INSERT_CLAIM = """
INSERT INTO wf_property_lossrun_claim
    (id, report_id, carrier_id,
     policy_number, policy_effective_date, policy_expiration_date,
     policy_limit, policy_deductible_amount, policy_deductible_type,
     coverage_line,
     claim_number, date_of_loss, cause_of_loss, loss_description, status,
     total_incurred, total_paid, outstanding_reserve,
     lae_reserve, lae_paid,
     deductible_applied, amount_less_deductible,
     notes, created_at, updated_at)
VALUES
    (gen_random_uuid(), %(report_id)s, %(carrier_id)s,
     %(policy_number)s, %(policy_effective_date)s, %(policy_expiration_date)s,
     %(policy_limit)s, %(policy_deductible_amount)s, %(policy_deductible_type)s,
     %(coverage_line)s,
     %(claim_number)s, %(date_of_loss)s, %(cause_of_loss)s, %(loss_description)s, %(status)s,
     %(total_incurred)s, %(total_paid)s, %(outstanding_reserve)s,
     %(lae_reserve)s, %(lae_paid)s,
     %(deductible_applied)s, %(amount_less_deductible)s,
     %(notes)s, now(), now())
RETURNING id;
"""

UPDATE_CLAIM_FINANCIALS = """
UPDATE wf_property_lossrun_claim
SET    total_incurred         = COALESCE(%(total_incurred)s,        total_incurred),
       total_paid             = COALESCE(%(total_paid)s,            total_paid),
       outstanding_reserve    = COALESCE(%(outstanding_reserve)s,   outstanding_reserve),
       lae_reserve            = COALESCE(%(lae_reserve)s,           lae_reserve),
       lae_paid               = COALESCE(%(lae_paid)s,              lae_paid),
       deductible_applied     = COALESCE(%(deductible_applied)s,    deductible_applied),
       amount_less_deductible = COALESCE(%(amount_less_deductible)s,amount_less_deductible),
       status                 = COALESCE(%(status)s,                status),
       notes                  = COALESCE(%(notes)s,                 notes),
       report_id              = %(report_id)s,
       updated_at             = now()
WHERE  id = %(id)s
RETURNING id;
"""

# ---------------------------------------------------------------------------
# Optional runtime constraint check (invoked by service.ensure_constraints).
# All of these are already declared inline in the CREATE TABLE statements;
# this block only matters if the schema was created without them.
# ---------------------------------------------------------------------------

REQUIRED_UNIQUE_CONSTRAINTS = """
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wf_plr_insured_name
    ON wf_property_lossrun_insured(name);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_wf_plr_broker_name
    ON wf_property_lossrun_broker(name);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_wf_plr_carrier_name
    ON wf_property_lossrun_carrier(name);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_wf_plr_property_name_address
    ON wf_property_lossrun_property(name, address);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_wf_plr_report_source_valuation
    ON wf_property_lossrun_report(source_file, valuation_date);
"""
