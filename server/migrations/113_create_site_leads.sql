-- 102_create_site_leads.sql

CREATE TABLE IF NOT EXISTS public.site_leads (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    contact TEXT NOT NULL,
    orders_volume TEXT NOT NULL,
    sku_count TEXT NOT NULL,
    comment TEXT,
    source TEXT DEFAULT 'landing',
    status TEXT NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы под будущую работу
CREATE INDEX IF NOT EXISTS idx_site_leads_created_at
ON public.site_leads (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_site_leads_status
ON public.site_leads (status);