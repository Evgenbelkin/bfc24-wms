BEGIN;

-- =========================================================
-- 1. Склады WB клиента, подтянутые по API
-- =========================================================
CREATE TABLE IF NOT EXISTS wms.client_wb_warehouses (
    id BIGSERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    mp_account_id BIGINT NULL,                 -- если у клиента несколько кабинетов WB
    warehouse_code TEXT NOT NULL,              -- код/ID склада из WB
    warehouse_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    weight NUMERIC(12,4) NOT NULL DEFAULT 1.0000,
    source TEXT NOT NULL DEFAULT 'wb_api',     -- wb_api / manual
    last_synced_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_client_wb_warehouse UNIQUE (client_id, mp_account_id, warehouse_code),
    CONSTRAINT chk_client_wb_warehouses_weight CHECK (weight >= 0)
);

CREATE INDEX IF NOT EXISTS idx_client_wb_warehouses_client_id
    ON wms.client_wb_warehouses(client_id);

CREATE INDEX IF NOT EXISTS idx_client_wb_warehouses_account_id
    ON wms.client_wb_warehouses(mp_account_id);

CREATE INDEX IF NOT EXISTS idx_client_wb_warehouses_active
    ON wms.client_wb_warehouses(client_id, is_active);


-- =========================================================
-- 2. Виртуальное распределение остатков по складам WB
--    Сумма по всем складам = фактическому остатку у тебя на складе
-- =========================================================
CREATE TABLE IF NOT EXISTS wms.client_stock_distribution (
    id BIGSERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    mp_account_id BIGINT NULL,
    barcode TEXT NOT NULL,
    warehouse_code TEXT NOT NULL,
    qty INTEGER NOT NULL DEFAULT 0,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_client_stock_distribution UNIQUE (client_id, mp_account_id, barcode, warehouse_code),
    CONSTRAINT chk_client_stock_distribution_qty CHECK (qty >= 0)
);

CREATE INDEX IF NOT EXISTS idx_client_stock_distribution_client_barcode
    ON wms.client_stock_distribution(client_id, barcode);

CREATE INDEX IF NOT EXISTS idx_client_stock_distribution_client_wh
    ON wms.client_stock_distribution(client_id, warehouse_code);

CREATE INDEX IF NOT EXISTS idx_client_stock_distribution_account_barcode
    ON wms.client_stock_distribution(mp_account_id, barcode);


-- =========================================================
-- 3. Лог синхронизации/перерасчёта остатков
--    Нужен чтобы видеть, когда и что обновлялось
-- =========================================================
CREATE TABLE IF NOT EXISTS wms.client_stock_distribution_runs (
    id BIGSERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    mp_account_id BIGINT NULL,
    trigger_type TEXT NOT NULL,                -- receiving / inventory / shipment / nightly / manual
    status TEXT NOT NULL DEFAULT 'success',    -- success / error
    items_count INTEGER NOT NULL DEFAULT 0,
    note TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_stock_distribution_runs_client
    ON wms.client_stock_distribution_runs(client_id, created_at DESC);


-- =========================================================
-- 4. Триггер updated_at
-- =========================================================
CREATE OR REPLACE FUNCTION wms.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_client_wb_warehouses_updated_at ON wms.client_wb_warehouses;
CREATE TRIGGER trg_client_wb_warehouses_updated_at
BEFORE UPDATE ON wms.client_wb_warehouses
FOR EACH ROW
EXECUTE FUNCTION wms.set_updated_at();

DROP TRIGGER IF EXISTS trg_client_stock_distribution_updated_at ON wms.client_stock_distribution;
CREATE TRIGGER trg_client_stock_distribution_updated_at
BEFORE UPDATE ON wms.client_stock_distribution
FOR EACH ROW
EXECUTE FUNCTION wms.set_updated_at();

COMMIT;