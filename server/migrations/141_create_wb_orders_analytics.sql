BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================================
-- 1. СЫРОЙ ИМПОРТ WB ЗАКАЗОВ ДЛЯ АНАЛИТИКИ
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.wb_orders_raw (
    id                      BIGSERIAL PRIMARY KEY,
    client_id               INTEGER NOT NULL,
    client_mp_account_id    INTEGER NOT NULL,
    source_type             TEXT NOT NULL DEFAULT 'wb',
    report_type             TEXT NOT NULL DEFAULT 'orders',
    source_order_id         TEXT,
    source_rid              TEXT,
    source_nm_id            BIGINT,
    source_chrt_id          BIGINT,
    article                 TEXT,
    barcode                 TEXT,
    warehouse_name          TEXT,
    region_name             TEXT,
    status_raw              TEXT,
    event_datetime          TIMESTAMPTZ,
    order_datetime          TIMESTAMPTZ,
    price_raw               NUMERIC(14,2),
    converted_price_raw     NUMERIC(14,2),
    final_price_raw         NUMERIC(14,2),
    converted_final_price_raw NUMERIC(14,2),
    raw                     JSONB NOT NULL,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_client_event_dt
    ON analytics.wb_orders_raw (client_id, event_datetime DESC);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_account_event_dt
    ON analytics.wb_orders_raw (client_mp_account_id, event_datetime DESC);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_order_dt
    ON analytics.wb_orders_raw (client_mp_account_id, order_datetime DESC);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_nm_id
    ON analytics.wb_orders_raw (source_nm_id);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_barcode
    ON analytics.wb_orders_raw (barcode);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_status
    ON analytics.wb_orders_raw (status_raw);

CREATE INDEX IF NOT EXISTS idx_wb_orders_raw_raw_gin
    ON analytics.wb_orders_raw USING GIN (raw);

CREATE UNIQUE INDEX IF NOT EXISTS ux_wb_orders_raw_source_unique
    ON analytics.wb_orders_raw (
        client_mp_account_id,
        COALESCE(source_order_id, ''),
        COALESCE(source_rid, '')
    );

-- =========================================================
-- 2. НОРМАЛИЗОВАННЫЕ WB ЗАКАЗЫ
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.wb_orders_normalized (
    id                          BIGSERIAL PRIMARY KEY,
    raw_id                      BIGINT NOT NULL REFERENCES analytics.wb_orders_raw(id) ON DELETE CASCADE,

    client_id                   INTEGER NOT NULL,
    client_mp_account_id        INTEGER NOT NULL,

    event_datetime              TIMESTAMPTZ NOT NULL,
    event_date                  DATE NOT NULL,
    order_datetime              TIMESTAMPTZ,
    order_date                  DATE NOT NULL,

    wb_order_id                 TEXT,
    rid                         TEXT,

    nm_id                       BIGINT,
    chrt_id                     BIGINT,
    article                     TEXT,
    barcode                     TEXT,

    warehouse_name              TEXT,
    region_name                 TEXT,
    status_raw                  TEXT,

    qty                         INTEGER NOT NULL DEFAULT 1,

    price_raw                   NUMERIC(14,2),
    converted_price_raw         NUMERIC(14,2),
    final_price_raw             NUMERIC(14,2),
    converted_final_price_raw   NUMERIC(14,2),

    order_amount                NUMERIC(14,2) NOT NULL DEFAULT 0,

    is_order                    BOOLEAN NOT NULL DEFAULT TRUE,

    raw                         JSONB NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_wb_orders_normalized_raw_id
    ON analytics.wb_orders_normalized (raw_id);

CREATE INDEX IF NOT EXISTS idx_wb_orders_normalized_client_order_date
    ON analytics.wb_orders_normalized (client_id, order_date DESC);

CREATE INDEX IF NOT EXISTS idx_wb_orders_normalized_account_order_date
    ON analytics.wb_orders_normalized (client_mp_account_id, order_date DESC);

CREATE INDEX IF NOT EXISTS idx_wb_orders_normalized_barcode
    ON analytics.wb_orders_normalized (barcode);

CREATE INDEX IF NOT EXISTS idx_wb_orders_normalized_article
    ON analytics.wb_orders_normalized (article);

CREATE INDEX IF NOT EXISTS idx_wb_orders_normalized_order_id
    ON analytics.wb_orders_normalized (wb_order_id);

-- =========================================================
-- 3. ТРИГГЕРЫ updated_at
-- =========================================================
DROP TRIGGER IF EXISTS trg_wb_orders_raw_updated_at ON analytics.wb_orders_raw;
CREATE TRIGGER trg_wb_orders_raw_updated_at
BEFORE UPDATE ON analytics.wb_orders_raw
FOR EACH ROW
EXECUTE FUNCTION analytics.set_updated_at();

DROP TRIGGER IF EXISTS trg_wb_orders_normalized_updated_at ON analytics.wb_orders_normalized;
CREATE TRIGGER trg_wb_orders_normalized_updated_at
BEFORE UPDATE ON analytics.wb_orders_normalized
FOR EACH ROW
EXECUTE FUNCTION analytics.set_updated_at();

COMMIT;