BEGIN;

-- -------------------------------------------------------
-- Добавляем возможность исключать склад из распределения
-- -------------------------------------------------------

ALTER TABLE wms.client_wb_warehouses
ADD COLUMN IF NOT EXISTS is_enabled_for_distribution BOOLEAN NOT NULL DEFAULT TRUE;

-- Индекс для быстрых выборок складов для распределения
CREATE INDEX IF NOT EXISTS idx_client_wb_warehouses_distribution
ON wms.client_wb_warehouses(client_id, mp_account_id, is_enabled_for_distribution);

COMMIT;