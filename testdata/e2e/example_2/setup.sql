create table file
(
    id uuid primary key default gen_random_uuid(),
    created_at timestamp with time zone default now(),
    filename text,
    mime_type text
);

create table file_identifier
(
    file_id uuid references file (id) on delete cascade,
    key text,
    value text
);

create table source
(
    id uuid primary key default gen_random_uuid(),
    file_id uuid references file (id) on delete cascade,
    title text,
    url text,
    accessed_at timestamp with time zone default now()
);

create table job
(
    id uuid primary key default gen_random_uuid(),
    created_at timestamp with time zone default now(),
    status text default 'pending',
    title text
);

create table entity
(
    id uuid primary key default gen_random_uuid(),
    job_id uuid references job (id) on delete cascade,
    created_at timestamp with time zone default now(),
    entity_type text,
    name text
);

create table entity_claim
(
    id uuid primary key default gen_random_uuid(),
    entity_id uuid references entity (id) on delete cascade,
    source_id uuid references source (id) on delete cascade,
    claim_type text,
    claim_value text
);

create table job_event
(
    id bigint primary key generated always as identity,
    job_id uuid references job (id) on delete cascade,
    "timestamp" timestamp with time zone default now(),
    message text
);

create table job_event_delivery
(
    job_id uuid primary key references job (id) on delete cascade,
    event_id bigint references job_event (id) on delete cascade,
    delivery_pending boolean default true,
    delivery_attempt_count integer default 0
);

create table schema_migrations
(
    version bigint primary key,
    dirty boolean
);

INSERT INTO job (id, created_at, status, title) VALUES
    ('aaaaaaaa-0000-0000-0000-000000000001', '2024-01-01 00:00:00+00', 'completed', 'Job A');

INSERT INTO job_event (job_id, "timestamp", message) VALUES
    ('aaaaaaaa-0000-0000-0000-000000000001', '2024-01-01 00:00:01+00', 'job completed');

INSERT INTO job_event_delivery (job_id, event_id, delivery_pending, delivery_attempt_count) VALUES
    ('aaaaaaaa-0000-0000-0000-000000000001', 1, false, 1);

INSERT INTO job (id, created_at, status, title) VALUES
    ('bbbbbbbb-0000-0000-0000-000000000002', '2024-01-02 00:00:00+00', 'completed', 'Job B');

INSERT INTO file (id, created_at, filename, mime_type) VALUES
    ('cccccccc-0000-0000-0001-000000000001', '2024-01-02 00:00:00+00', 'report.pdf', 'application/pdf');

INSERT INTO file_identifier (file_id, key, value) VALUES
    ('cccccccc-0000-0000-0001-000000000001', 'url', 'https://example.com/report.pdf');

INSERT INTO source (id, file_id, title, url, accessed_at) VALUES
    ('dddddddd-0000-0000-0002-000000000001', 'cccccccc-0000-0000-0001-000000000001', 'Company Register', 'https://example.com/register', '2024-01-02 00:00:00+00');

INSERT INTO entity (id, job_id, created_at, entity_type, name) VALUES
    ('eeeeeeee-0000-0000-0003-000000000001', 'bbbbbbbb-0000-0000-0000-000000000002', '2024-01-02 00:00:00+00', 'legal_person', 'Example GmbH');

INSERT INTO entity_claim (id, entity_id, source_id, claim_type, claim_value) VALUES
    ('ffffffff-0000-0000-0004-000000000001', 'eeeeeeee-0000-0000-0003-000000000001', 'dddddddd-0000-0000-0002-000000000001', 'register_code', 'HRB 12345');
