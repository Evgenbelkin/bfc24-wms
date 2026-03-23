BEGIN;

CREATE TABLE IF NOT EXISTS wms.seller_item_settings (
    id bigserial PRIMARY KEY,
    client_id bigint NOT NULL,
    mp_account_id bigint NULL,
    barcode text NOT NULL,
    low_stock_threshold integer NOT NULL DEFAULT 0,
    target_stock integer NULL,
    is_monitoring_enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT seller_item_settings_client_barcode_uq UNIQUE (client_id, barcode)
);

CREATE INDEX IF NOT EXISTS idx_seller_item_settings_client_id
    ON wms.seller_item_settings (client_id);

CREATE INDEX IF NOT EXISTS idx_seller_item_settings_barcode
    ON wms.seller_item_settings (barcode);

CREATE INDEX IF NOT EXISTS idx_seller_item_settings_client_mp
    ON wms.seller_item_settings (client_id, mp_account_id);

COMMIT;