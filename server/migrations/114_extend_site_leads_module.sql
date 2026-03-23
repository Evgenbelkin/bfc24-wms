-- 103_extend_site_leads_module.sql

ALTER TABLE public.site_leads
ADD COLUMN IF NOT EXISTS assigned_user_id BIGINT,
ADD COLUMN IF NOT EXISTS next_action TEXT,
ADD COLUMN IF NOT EXISTS next_contact_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE public.site_leads
ALTER COLUMN status SET DEFAULT 'new';

CREATE TABLE IF NOT EXISTS public.site_lead_comments (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL REFERENCES public.site_leads(id) ON DELETE CASCADE,
    comment_text TEXT NOT NULL,
    created_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_site_leads_status
ON public.site_leads(status);

CREATE INDEX IF NOT EXISTS idx_site_leads_created_at
ON public.site_leads(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_site_leads_next_contact_at
ON public.site_leads(next_contact_at);

CREATE INDEX IF NOT EXISTS idx_site_lead_comments_lead_id
ON public.site_lead_comments(lead_id, created_at DESC);

UPDATE public.site_leads
SET updated_at = created_at
WHERE updated_at IS NULL;