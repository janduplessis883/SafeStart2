create extension if not exists pgcrypto;

create table if not exists public.surgeries (
    id uuid primary key default gen_random_uuid(),
    surgery_code text not null unique,
    surgery_name text not null,
    sms_sender_id text,
    email text,
    phone text,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.surgery_users (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid references public.surgeries(id) on delete cascade,
    email text not null unique,
    full_name text not null,
    role text not null default 'admin' check (role in ('superuser', 'admin', 'user')),
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.import_batches (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    uploaded_by_email text,
    source_filename text not null,
    row_count integer not null default 0,
    patient_count integer not null default 0,
    recommendation_count integer not null default 0,
    unvaccinated_count integer not null default 0,
    imported_at timestamptz not null default now(),
    notes text
);

create table if not exists public.patients (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    nhs_number text not null,
    source_patient_id text,
    first_name text not null,
    last_name text not null,
    full_name text not null,
    sex text,
    date_of_birth date not null,
    phone text,
    email text,
    registration_date date,
    is_unvaccinated boolean not null default false,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (surgery_id, nhs_number)
);

create index if not exists idx_patients_surgery_dob
    on public.patients (surgery_id, date_of_birth);

create table if not exists public.import_rows (
    id uuid primary key default gen_random_uuid(),
    batch_id uuid not null references public.import_batches(id) on delete cascade,
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    nhs_number text,
    source_patient_id text,
    raw_vaccine_name text,
    raw_event_date text,
    parse_status text not null default 'accepted' check (parse_status in ('accepted', 'warning', 'rejected')),
    parse_message text,
    raw_payload jsonb not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_import_rows_batch
    on public.import_rows (batch_id);

create table if not exists public.vaccination_events (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    patient_id uuid not null references public.patients(id) on delete cascade,
    batch_id uuid references public.import_batches(id) on delete set null,
    canonical_vaccine text not null,
    vaccine_program text not null,
    raw_vaccine_name text not null,
    event_date date,
    event_done_at_id text,
    source_hash text not null,
    created_at timestamptz not null default now(),
    unique (surgery_id, source_hash)
);

create index if not exists idx_vaccination_events_patient
    on public.vaccination_events (patient_id, canonical_vaccine, event_date);

create index if not exists idx_vaccination_events_batch
    on public.vaccination_events (batch_id);

create table if not exists public.recall_recommendations (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    patient_id uuid not null references public.patients(id) on delete cascade,
    batch_id uuid references public.import_batches(id) on delete set null,
    recommendation_type text not null check (recommendation_type in ('routine', 'seasonal', 'unvaccinated', 'review')),
    vaccine_group text not null,
    program_area text not null,
    due_date date not null,
    status text not null check (status in ('due_soon', 'due_now', 'overdue', 'unvaccinated', 'review', 'complete', 'suppressed', 'resolved_by_vaccination')),
    priority integer not null default 50,
    reason text not null,
    explanation jsonb not null default '{}'::jsonb,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (surgery_id, patient_id, vaccine_group, recommendation_type, due_date)
);

create index if not exists idx_recall_recommendations_active
    on public.recall_recommendations (surgery_id, is_active, status, priority);

create index if not exists idx_recall_recommendations_batch
    on public.recall_recommendations (batch_id);

create table if not exists public.bulk_sms_batches (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    prepared_by_email text,
    prepared_by_name text,
    status text not null default 'prepared' check (status in ('prepared', 'sent', 'delivered', 'failed', 'booked', 'declined', 'no_response')),
    ready_count integer not null default 0,
    blocked_count integer not null default 0,
    selection_summary jsonb not null default '{}'::jsonb,
    export_rows jsonb not null default '[]'::jsonb,
    blocked_rows jsonb not null default '[]'::jsonb,
    self_book_url text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_bulk_sms_batches_surgery_created
    on public.bulk_sms_batches (surgery_id, created_at desc);

create table if not exists public.recall_batches (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    prepared_by_email text,
    prepared_by_name text,
    delivery_method text check (delivery_method in ('sms', 'email', 'letter', 'call')),
    status text not null default 'prepared' check (status in ('prepared', 'sent', 'delivered', 'failed', 'booked', 'declined', 'no_response')),
    selected_count integer not null default 0,
    ready_count integer not null default 0,
    blocked_count integer not null default 0,
    selection_summary jsonb not null default '{}'::jsonb,
    export_rows jsonb not null default '[]'::jsonb,
    blocked_rows jsonb not null default '[]'::jsonb,
    self_book_url text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_recall_batches_surgery_created
    on public.recall_batches (surgery_id, created_at desc);

create table if not exists public.recall_attempts (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid not null references public.surgeries(id) on delete cascade,
    recommendation_id uuid not null references public.recall_recommendations(id) on delete cascade,
    bulk_sms_batch_id uuid references public.bulk_sms_batches(id) on delete set null,
    recall_batch_id uuid references public.recall_batches(id) on delete set null,
    communication_method text not null check (communication_method in ('sms', 'email', 'letter', 'call', 'bulk_sms')),
    staff_member text,
    sent_at timestamptz not null default now(),
    outcome text not null default 'sent',
    notes text
);

create index if not exists idx_recall_attempts_bulk_sms_batch
    on public.recall_attempts (bulk_sms_batch_id);

create index if not exists idx_recall_attempts_recall_batch
    on public.recall_attempts (recall_batch_id);

create table if not exists public.vaccine_alias_overrides (
    id uuid primary key default gen_random_uuid(),
    surgery_id uuid references public.surgeries(id) on delete cascade,
    raw_label text not null,
    canonical_vaccine text not null,
    vaccine_program text not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now()
);

create unique index if not exists idx_vaccine_alias_overrides_scope_label
    on public.vaccine_alias_overrides (
        coalesce(surgery_id, '00000000-0000-0000-0000-000000000000'::uuid),
        raw_label
    );

create or replace view public.v_active_recalls as
select
    rr.id,
    rr.surgery_id,
    p.nhs_number,
    p.full_name,
    p.date_of_birth,
    p.phone,
    rr.recommendation_type,
    rr.vaccine_group,
    rr.program_area,
    rr.due_date,
    rr.status,
    rr.priority,
    rr.reason,
    rr.explanation,
    rr.updated_at,
    p.email
from public.recall_recommendations rr
join public.patients p on p.id = rr.patient_id
where rr.is_active = true
  and rr.status in ('due_soon', 'due_now', 'overdue', 'unvaccinated', 'review');

create or replace function public.set_timestamp()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_surgeries_updated_at on public.surgeries;
create trigger trg_surgeries_updated_at
before update on public.surgeries
for each row execute function public.set_timestamp();

drop trigger if exists trg_surgery_users_updated_at on public.surgery_users;
create trigger trg_surgery_users_updated_at
before update on public.surgery_users
for each row execute function public.set_timestamp();

drop trigger if exists trg_patients_updated_at on public.patients;
create trigger trg_patients_updated_at
before update on public.patients
for each row execute function public.set_timestamp();

drop trigger if exists trg_recall_recommendations_updated_at on public.recall_recommendations;
create trigger trg_recall_recommendations_updated_at
before update on public.recall_recommendations
for each row execute function public.set_timestamp();

drop trigger if exists trg_bulk_sms_batches_updated_at on public.bulk_sms_batches;
create trigger trg_bulk_sms_batches_updated_at
before update on public.bulk_sms_batches
for each row execute function public.set_timestamp();

drop trigger if exists trg_recall_batches_updated_at on public.recall_batches;
create trigger trg_recall_batches_updated_at
before update on public.recall_batches
for each row execute function public.set_timestamp();
