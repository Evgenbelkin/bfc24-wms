BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================================
-- 1. СЫРОЙ ИМПОРТ ДАННЫХ WB ДЛЯ АНАЛИТИКИ ПРОДАЖ
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.wb_sales_raw (
    id                      BIGSERIAL PRIMARY KEY,
    client_id               INTEGER NOT NULL,
    client_mp_account_id    INTEGER NOT NULL,
    source_type             TEXT NOT NULL DEFAULT 'wb',
    report_type             TEXT NOT NULL, -- sales / orders / realizations / incomes / stocks / unknown
    source_record_id        TEXT,
    source_event_id         TEXT,
    source_order_id         TEXT,
    source_sale_id          TEXT,
    source_rid             TEXT,
    source_nm_id            BIGINT,
    source_chrt_id          BIGINT,
    article                 TEXT,
    barcode                 TEXT,
    subject                 TEXT,
    brand                   TEXT,
    warehouse_name          TEXT,
    region_name             TEXT,
    country_name            TEXT,
    status_raw              TEXT,
    event_datetime          TIMESTAMPTZ,
    sale_datetime           TIMESTAMPTZ,
    cancel_datetime         TIMESTAMPTZ,
    return_datetime         TIMESTAMPTZ,
    price_raw               NUMERIC(14,2),
    sale_price_raw          NUMERIC(14,2),
    final_price_raw         NUMERIC(14,2),
    discount_percent_raw    NUMERIC(10,2),
    spp_raw                 NUMERIC(10,2),
    for_pay_raw             NUMERIC(14,2),
    finished_price_raw      NUMERIC(14,2),
    raw                     JSONB NOT NULL,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_client_event_dt
    ON analytics.wb_sales_raw (client_id, event_datetime DESC);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_account_event_dt
    ON analytics.wb_sales_raw (client_mp_account_id, event_datetime DESC);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_report_type
    ON analytics.wb_sales_raw (report_type);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_status_raw
    ON analytics.wb_sales_raw (status_raw);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_nm_id
    ON analytics.wb_sales_raw (source_nm_id);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_barcode
    ON analytics.wb_sales_raw (barcode);

CREATE INDEX IF NOT EXISTS idx_wb_sales_raw_raw_gin
    ON analytics.wb_sales_raw USING GIN (raw);

CREATE UNIQUE INDEX IF NOT EXISTS ux_wb_sales_raw_source_unique
    ON analytics.wb_sales_raw (
        client_mp_account_id,
        report_type,
        COALESCE(source_record_id, ''),
        COALESCE(source_event_id, ''),
        COALESCE(source_order_id, ''),
        COALESCE(source_sale_id, ''),
        COALESCE(source_rid, '')
    );

-- =========================================================
-- 2. НОРМАЛИЗОВАННЫЕ СОБЫТИЯ ДЛЯ АНАЛИТИКИ
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.wb_sales_normalized (
    id                          BIGSERIAL PRIMARY KEY,
    raw_id                       BIGINT NOT NULL REFERENCES analytics.wb_sales_raw(id) ON DELETE CASCADE,

    client_id                    INTEGER NOT NULL,
    client_mp_account_id         INTEGER NOT NULL,

    report_type                  TEXT NOT NULL,
    event_type                   TEXT NOT NULL,      -- sale / return / cancel / order / unknown
    status_raw                   TEXT,
    status_normalized            TEXT NOT NULL,      -- sale / return / cancel / order / unknown

    event_datetime               TIMESTAMPTZ NOT NULL,
    event_date                   DATE NOT NULL,

    sale_datetime                TIMESTAMPTZ,
    cancel_datetime              TIMESTAMPTZ,
    return_datetime              TIMESTAMPTZ,

    wb_order_id                  TEXT,
    wb_sale_id                   TEXT,
    rid                          TEXT,

    nm_id                        BIGINT,
    chrt_id                      BIGINT,
    article                      TEXT,
    barcode                      TEXT,
    subject                      TEXT,
    brand                        TEXT,

    warehouse_name               TEXT,
    region_name                  TEXT,
    country_name                 TEXT,

    qty                          INTEGER NOT NULL DEFAULT 1,

    price_raw                    NUMERIC(14,2),
    sale_price_raw               NUMERIC(14,2),
    final_price_raw              NUMERIC(14,2),
    discount_percent_raw         NUMERIC(10,2),
    spp_raw                      NUMERIC(10,2),
    for_pay_raw                  NUMERIC(14,2),
    finished_price_raw           NUMERIC(14,2),

    amount_gross                 NUMERIC(14,2),
    amount_net                   NUMERIC(14,2),

    is_sale                      BOOLEAN NOT NULL DEFAULT FALSE,
    is_return                    BOOLEAN NOT NULL DEFAULT FALSE,
    is_cancel                    BOOLEAN NOT NULL DEFAULT FALSE,
    is_order                     BOOLEAN NOT NULL DEFAULT FALSE,

    raw                          JSONB NOT NULL,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_wb_sales_normalized_event_type
        CHECK (event_type IN ('sale', 'return', 'cancel', 'order', 'unknown')),

    CONSTRAINT chk_wb_sales_normalized_status_normalized
        CHECK (status_normalized IN ('sale', 'return', 'cancel', 'order', 'unknown'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_wb_sales_normalized_raw_id
    ON analytics.wb_sales_normalized (raw_id);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_client_event_date
    ON analytics.wb_sales_normalized (client_id, event_date DESC);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_client_event_type
    ON analytics.wb_sales_normalized (client_id, event_type, event_date DESC);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_account_event_date
    ON analytics.wb_sales_normalized (client_mp_account_id, event_date DESC);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_nm_id
    ON analytics.wb_sales_normalized (nm_id);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_barcode
    ON analytics.wb_sales_normalized (barcode);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_article
    ON analytics.wb_sales_normalized (article);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_order_id
    ON analytics.wb_sales_normalized (wb_order_id);

CREATE INDEX IF NOT EXISTS idx_wb_sales_normalized_sale_id
    ON analytics.wb_sales_normalized (wb_sale_id);

-- =========================================================
-- 3. ТЕХНИЧЕСКАЯ ТАБЛИЦА ЗАПУСКОВ СИНХРОНИЗАЦИИ
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.wb_sales_sync_runs (
    id                      BIGSERIAL PRIMARY KEY,
    client_id               INTEGER,
    client_mp_account_id    INTEGER,
    report_type             TEXT NOT NULL,
    date_from               DATE,
    date_to                 DATE,
    status                  TEXT NOT NULL DEFAULT 'running', -- running / success / error
    rows_received           INTEGER NOT NULL DEFAULT 0,
    rows_inserted_raw       INTEGER NOT NULL DEFAULT 0,
    rows_updated_raw        INTEGER NOT NULL DEFAULT 0,
    rows_normalized         INTEGER NOT NULL DEFAULT 0,
    error_text              TEXT,
    started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_wb_sales_sync_runs_account_started
    ON analytics.wb_sales_sync_runs (client_mp_account_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_wb_sales_sync_runs_status
    ON analytics.wb_sales_sync_runs (status, started_at DESC);

-- =========================================================
-- 4. ТРИГГЕР updated_at
-- =========================================================
CREATE OR REPLACE FUNCTION analytics.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_wb_sales_raw_updated_at ON analytics.wb_sales_raw;
CREATE TRIGGER trg_wb_sales_raw_updated_at
BEFORE UPDATE ON analytics.wb_sales_raw
FOR EACH ROW
EXECUTE FUNCTION analytics.set_updated_at();

DROP TRIGGER IF EXISTS trg_wb_sales_normalized_updated_at ON analytics.wb_sales_normalized;
CREATE TRIGGER trg_wb_sales_normalized_updated_at
BEFORE UPDATE ON analytics.wb_sales_normalized
FOR EACH ROW
EXECUTE FUNCTION analytics.set_updated_at();

COMMIT;