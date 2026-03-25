BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================================
-- 1. analytics.sales_orders
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.sales_orders (
    id BIGSERIAL PRIMARY KEY,

    client_id INTEGER NOT NULL,
    mp_account_id INTEGER NOT NULL,

    marketplace VARCHAR(30) NOT NULL DEFAULT 'wb',
    fulfillment_model VARCHAR(20) NOT NULL DEFAULT 'unknown',

    external_order_id VARCHAR(100) NOT NULL,
    order_code VARCHAR(150),

    order_date TIMESTAMPTZ NOT NULL,
    sale_date TIMESTAMPTZ NULL,

    status VARCHAR(50) NOT NULL DEFAULT 'new',
    currency_code VARCHAR(10) NOT NULL DEFAULT 'RUB',

    items_count INTEGER NOT NULL DEFAULT 0,
    total_amount NUMERIC(14,2) NOT NULL DEFAULT 0,

    source_payload JSONB,
    source_system VARCHAR(50) NOT NULL DEFAULT 'wb_api',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_analytics_sales_orders_marketplace
        CHECK (marketplace IN ('wb')),

    CONSTRAINT chk_analytics_sales_orders_fulfillment_model
        CHECK (fulfillment_model IN ('fbs', 'fbo', 'unknown')),

    CONSTRAINT chk_analytics_sales_orders_items_count
        CHECK (items_count >= 0),

    CONSTRAINT chk_analytics_sales_orders_total_amount
        CHECK (total_amount >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_analytics_sales_orders_unique_external
    ON analytics.sales_orders (marketplace, mp_account_id, external_order_id);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_orders_client_date
    ON analytics.sales_orders (client_id, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_orders_client_mp_date
    ON analytics.sales_orders (client_id, mp_account_id, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_orders_mp_date
    ON analytics.sales_orders (mp_account_id, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_orders_fulfillment_date
    ON analytics.sales_orders (fulfillment_model, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_orders_status
    ON analytics.sales_orders (status);

-- =========================================================
-- 2. analytics.sales_order_lines
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.sales_order_lines (
    id BIGSERIAL PRIMARY KEY,

    sales_order_id BIGINT NOT NULL REFERENCES analytics.sales_orders(id) ON DELETE CASCADE,

    client_id INTEGER NOT NULL,
    mp_account_id INTEGER NOT NULL,

    marketplace VARCHAR(30) NOT NULL DEFAULT 'wb',
    fulfillment_model VARCHAR(20) NOT NULL DEFAULT 'unknown',

    external_line_id VARCHAR(120),

    barcode VARCHAR(100),
    sku_id INTEGER,
    nm_id BIGINT,

    vendor_code VARCHAR(120),
    wb_vendor_code VARCHAR(120),
    item_name TEXT NOT NULL,

    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(14,2) NOT NULL DEFAULT 0,
    final_price NUMERIC(14,2) NOT NULL DEFAULT 0,
    line_amount NUMERIC(14,2) NOT NULL DEFAULT 0,

    order_date TIMESTAMPTZ NOT NULL,

    source_payload JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_analytics_sales_order_lines_marketplace
        CHECK (marketplace IN ('wb')),

    CONSTRAINT chk_analytics_sales_order_lines_fulfillment_model
        CHECK (fulfillment_model IN ('fbs', 'fbo', 'unknown')),

    CONSTRAINT chk_analytics_sales_order_lines_quantity
        CHECK (quantity >= 0),

    CONSTRAINT chk_analytics_sales_order_lines_unit_price
        CHECK (unit_price >= 0),

    CONSTRAINT chk_analytics_sales_order_lines_final_price
        CHECK (final_price >= 0),

    CONSTRAINT chk_analytics_sales_order_lines_line_amount
        CHECK (line_amount >= 0)
);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_sales_order_id
    ON analytics.sales_order_lines (sales_order_id);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_client_date
    ON analytics.sales_order_lines (client_id, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_client_mp_date
    ON analytics.sales_order_lines (client_id, mp_account_id, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_client_barcode_date
    ON analytics.sales_order_lines (client_id, barcode, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_client_vendor_code_date
    ON analytics.sales_order_lines (client_id, vendor_code, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_client_wb_vendor_code_date
    ON analytics.sales_order_lines (client_id, wb_vendor_code, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_fulfillment_date
    ON analytics.sales_order_lines (fulfillment_model, order_date DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sales_order_lines_nm_id
    ON analytics.sales_order_lines (nm_id);

-- =========================================================
-- 3. analytics.sync_runs
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.sync_runs (
    id BIGSERIAL PRIMARY KEY,

    client_id INTEGER,
    mp_account_id INTEGER,

    marketplace VARCHAR(30) NOT NULL DEFAULT 'wb',
    fulfillment_model VARCHAR(20),

    sync_type VARCHAR(50) NOT NULL,
    date_from TIMESTAMPTZ,
    date_to TIMESTAMPTZ,

    status VARCHAR(30) NOT NULL DEFAULT 'new',
    rows_loaded INTEGER NOT NULL DEFAULT 0,

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    error_text TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_analytics_sync_runs_marketplace
        CHECK (marketplace IN ('wb')),

    CONSTRAINT chk_analytics_sync_runs_fulfillment_model
        CHECK (fulfillment_model IN ('fbs', 'fbo', 'unknown') OR fulfillment_model IS NULL),

    CONSTRAINT chk_analytics_sync_runs_status
        CHECK (status IN ('new', 'running', 'success', 'partial', 'failed')),

    CONSTRAINT chk_analytics_sync_runs_rows_loaded
        CHECK (rows_loaded >= 0)
);

CREATE INDEX IF NOT EXISTS ix_analytics_sync_runs_client_created
    ON analytics.sync_runs (client_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sync_runs_client_mp_created
    ON analytics.sync_runs (client_id, mp_account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sync_runs_sync_type_created
    ON analytics.sync_runs (sync_type, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_analytics_sync_runs_status_created
    ON analytics.sync_runs (status, created_at DESC);

-- =========================================================
-- 4. comments
-- =========================================================
COMMENT ON SCHEMA analytics IS 'Схема аналитики кабинета селлера и BI-агрегаций';

COMMENT ON TABLE analytics.sales_orders IS 'Шапки заказов/продаж для аналитики кабинета селлера';
COMMENT ON TABLE analytics.sales_order_lines IS 'Строки заказов/продаж для аналитики кабинета селлера';
COMMENT ON TABLE analytics.sync_runs IS 'Журнал запусков синхронизации аналитических данных';

COMMENT ON COLUMN analytics.sales_orders.fulfillment_model IS 'Модель продаж: fbs, fbo, unknown';
COMMENT ON COLUMN analytics.sales_order_lines.fulfillment_model IS 'Модель продаж строки: fbs, fbo, unknown';
COMMENT ON COLUMN analytics.sync_runs.fulfillment_model IS 'Какая модель продаж синхронизировалась: fbs, fbo, unknown или NULL для смешанного запуска';

COMMIT;