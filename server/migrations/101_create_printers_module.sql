-- Таблица принтеров
CREATE TABLE IF NOT EXISTS wms.printers (
    id bigserial PRIMARY KEY,
    printer_code text NOT NULL UNIQUE,
    printer_name text NOT NULL,
    printer_type text NOT NULL DEFAULT 'label',
    connection_type text NOT NULL DEFAULT 'agent',
    agent_code text,
    device_name text,
    ip_address text,
    port integer,
    warehouse_code text,
    zone_code text,
    is_default boolean NOT NULL DEFAULT false,
    is_active boolean NOT NULL DEFAULT true,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Таблица маршрутов печати
CREATE TABLE IF NOT EXISTS wms.printer_routes (
    id bigserial PRIMARY KEY,
    route_code text NOT NULL UNIQUE,
    doc_type text NOT NULL,
    warehouse_code text,
    zone_code text,
    client_id integer,
    printer_id bigint NOT NULL REFERENCES wms.printers(id),
    is_default boolean NOT NULL DEFAULT false,
    is_active boolean NOT NULL DEFAULT true,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Таблица журнала печати
CREATE TABLE IF NOT EXISTS wms.print_jobs (
    id bigserial PRIMARY KEY,
    job_code text,
    printer_id bigint REFERENCES wms.printers(id),
    doc_type text NOT NULL,
    entity_type text,
    entity_id text,
    copies integer NOT NULL DEFAULT 1,
    payload_json jsonb,
    status text NOT NULL DEFAULT 'queued',
    error_text text,
    created_by integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz,
    printed_at timestamptz
);

-- Индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_printers_active
ON wms.printers (is_active);

CREATE INDEX IF NOT EXISTS idx_printer_routes_doc_type
ON wms.printer_routes (doc_type, is_active);

CREATE INDEX IF NOT EXISTS idx_print_jobs_status
ON wms.print_jobs (status, created_at);