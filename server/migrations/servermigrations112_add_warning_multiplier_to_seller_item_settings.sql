BEGIN;

ALTER TABLE wms.seller_item_settings
ADD COLUMN IF NOT EXISTS warning_multiplier numeric(5,2);

-- дефолт не ставим в колонке, чтобы можно было использовать fallback = 1.5

COMMIT;