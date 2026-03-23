-- =====================================================
-- 102_add_client_id_to_auth_users.sql
-- добавляем привязку пользователя к клиенту (для селлеров)
-- =====================================================

ALTER TABLE auth.users
ADD COLUMN IF NOT EXISTS client_id INTEGER;

-- индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_auth_users_client_id
ON auth.users(client_id);

-- внешний ключ на клиентов
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_auth_users_client_id'
  ) THEN
    ALTER TABLE auth.users
    ADD CONSTRAINT fk_auth_users_client_id
    FOREIGN KEY (client_id)
    REFERENCES masterdata.clients(id)
    ON DELETE SET NULL;
  END IF;
END
$$;