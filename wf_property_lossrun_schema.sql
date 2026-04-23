-- =============================================================================
-- WF Property Loss Run Database Schema (flattened)
-- PostgreSQL DDL
--
-- Design:
--   Reference entities (property, insured, broker, carrier) stay normalized
--   because they are legitimately reused across reports. Policy + coverage-
--   type fields collapse onto each claim row since every claim belongs to
--   exactly one policy. `report` is retained as a thin document header so
--   no-loss letters (which have zero claims) can still be recorded and
--   (source_file, valuation_date) remains the idempotency key.
--
-- Dropped from the previous design:
--   - wf_property_lossrun_policy        (merged into claim)
--   - wf_property_lossrun_coverage_type (now a CHECK on claim.coverage_line)
--   - wf_property_lossrun_property_policy (not needed at single-location grain)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()


-- -----------------------------------------------------------------------------
-- REFERENCE TABLES
-- -----------------------------------------------------------------------------

CREATE TABLE wf_property_lossrun_property (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200)    NOT NULL,
    address         VARCHAR(255)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    state           CHAR(2)         NOT NULL,
    zip             VARCHAR(10),
    property_type   VARCHAR(100),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (name, address)
);


CREATE TABLE wf_property_lossrun_insured (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200)    NOT NULL UNIQUE,
    contact_name    VARCHAR(150),
    phone           VARCHAR(30),
    email           VARCHAR(150),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);


CREATE TABLE wf_property_lossrun_broker (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200)    NOT NULL UNIQUE,
    contact_name    VARCHAR(150),
    phone           VARCHAR(30),
    email           VARCHAR(150),
    license_number  VARCHAR(50),
    address         VARCHAR(255),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);


CREATE TABLE wf_property_lossrun_carrier (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200)    NOT NULL UNIQUE,
    naic_code       VARCHAR(10),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);


-- -----------------------------------------------------------------------------
-- DOCUMENT HEADER
-- -----------------------------------------------------------------------------

CREATE TABLE wf_property_lossrun_report (
    id                  UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id         UUID            NOT NULL REFERENCES wf_property_lossrun_property(id),
    insured_id          UUID            NOT NULL REFERENCES wf_property_lossrun_insured(id),
    broker_id           UUID            REFERENCES wf_property_lossrun_broker(id),
    valuation_date      DATE,
    report_date         DATE,
    period_start        DATE,
    period_end          DATE,
    document_type       VARCHAR(50)     DEFAULT 'Loss Run',
    source_file         VARCHAR(500),
    source_system       VARCHAR(100),
    no_loss_confirmed   BOOLEAN         NOT NULL DEFAULT FALSE,
    no_loss_as_of_date  DATE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (source_file, valuation_date)
);

COMMENT ON TABLE wf_property_lossrun_report IS
    'One row per ingested document. No-loss letters live here with zero claims.';


-- -----------------------------------------------------------------------------
-- FLAT CLAIM FACT
-- -----------------------------------------------------------------------------

CREATE TABLE wf_property_lossrun_claim (
    id                          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id                   UUID            NOT NULL REFERENCES wf_property_lossrun_report(id),
    carrier_id                  UUID            REFERENCES wf_property_lossrun_carrier(id),

    -- Policy attributes (flattened from the old policy table)
    policy_number               VARCHAR(100),
    policy_effective_date       DATE,
    policy_expiration_date      DATE,
    policy_limit                NUMERIC(18,2),
    policy_deductible_amount    NUMERIC(18,2),
    policy_deductible_type      VARCHAR(50),

    -- Coverage line (was its own lookup table; now a CHECK)
    coverage_line               VARCHAR(50)     NOT NULL
        CHECK (coverage_line IN ('Property', 'General Liability', 'Commercial General Liability')),

    -- Claim attributes
    claim_number                VARCHAR(100),
    date_of_loss                DATE,
    cause_of_loss               VARCHAR(200),
    loss_description            TEXT,
    status                      VARCHAR(20),

    -- Financials
    total_incurred              NUMERIC(18,2),
    total_paid                  NUMERIC(18,2),
    outstanding_reserve         NUMERIC(18,2),
    lae_reserve                 NUMERIC(18,2),
    lae_paid                    NUMERIC(18,2),
    deductible_applied          NUMERIC(18,2),
    amount_less_deductible      NUMERIC(18,2),

    notes                       TEXT,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

COMMENT ON TABLE wf_property_lossrun_claim IS
    'Flat claim rows. Each row is self-contained with policy, coverage, and
     financials inline. De-dup within a report on (report_id, claim_number,
     date_of_loss); across reports for the same property via the same tuple.';


-- -----------------------------------------------------------------------------
-- INDEXES
-- -----------------------------------------------------------------------------

CREATE INDEX idx_wf_plr_property_state       ON wf_property_lossrun_property(state);

CREATE INDEX idx_wf_plr_report_property      ON wf_property_lossrun_report(property_id);
CREATE INDEX idx_wf_plr_report_insured       ON wf_property_lossrun_report(insured_id);
CREATE INDEX idx_wf_plr_report_broker        ON wf_property_lossrun_report(broker_id);
CREATE INDEX idx_wf_plr_report_valuation     ON wf_property_lossrun_report(valuation_date);

CREATE INDEX idx_wf_plr_claim_report         ON wf_property_lossrun_claim(report_id);
CREATE INDEX idx_wf_plr_claim_carrier        ON wf_property_lossrun_claim(carrier_id);
CREATE INDEX idx_wf_plr_claim_date_of_loss   ON wf_property_lossrun_claim(date_of_loss);
CREATE INDEX idx_wf_plr_claim_status         ON wf_property_lossrun_claim(status);
CREATE INDEX idx_wf_plr_claim_coverage_line  ON wf_property_lossrun_claim(coverage_line);
CREATE INDEX idx_wf_plr_claim_number         ON wf_property_lossrun_claim(claim_number)
    WHERE claim_number IS NOT NULL;
CREATE INDEX idx_wf_plr_claim_policy_number  ON wf_property_lossrun_claim(policy_number)
    WHERE policy_number IS NOT NULL;


-- -----------------------------------------------------------------------------
-- UPDATED_AT TRIGGER
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION wf_property_lossrun_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_wf_plr_property_updated_at
    BEFORE UPDATE ON wf_property_lossrun_property
    FOR EACH ROW EXECUTE FUNCTION wf_property_lossrun_set_updated_at();

CREATE TRIGGER trg_wf_plr_insured_updated_at
    BEFORE UPDATE ON wf_property_lossrun_insured
    FOR EACH ROW EXECUTE FUNCTION wf_property_lossrun_set_updated_at();

CREATE TRIGGER trg_wf_plr_broker_updated_at
    BEFORE UPDATE ON wf_property_lossrun_broker
    FOR EACH ROW EXECUTE FUNCTION wf_property_lossrun_set_updated_at();

CREATE TRIGGER trg_wf_plr_carrier_updated_at
    BEFORE UPDATE ON wf_property_lossrun_carrier
    FOR EACH ROW EXECUTE FUNCTION wf_property_lossrun_set_updated_at();

CREATE TRIGGER trg_wf_plr_report_updated_at
    BEFORE UPDATE ON wf_property_lossrun_report
    FOR EACH ROW EXECUTE FUNCTION wf_property_lossrun_set_updated_at();

CREATE TRIGGER trg_wf_plr_claim_updated_at
    BEFORE UPDATE ON wf_property_lossrun_claim
    FOR EACH ROW EXECUTE FUNCTION wf_property_lossrun_set_updated_at();
