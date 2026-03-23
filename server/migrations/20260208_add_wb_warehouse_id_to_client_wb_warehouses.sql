-- Добавляем поле wb_warehouse_id
ALTER TABLE wms.client_wb_warehouses
ADD COLUMN IF NOT EXISTS wb_warehouse_id bigint;

-- Индекс для ускорения
CREATE INDEX IF NOT EXISTS idx_client_wb_warehouses_wb_warehouse_id
ON wms.client_wb_warehouses (wb_warehouse_id);