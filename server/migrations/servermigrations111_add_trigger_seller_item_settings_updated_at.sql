BEGIN;

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_seller_item_settings_updated_at ON wms.seller_item_settings;

CREATE TRIGGER trg_seller_item_settings_updated_at
BEFORE UPDATE ON wms.seller_item_settings
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

COMMIT;