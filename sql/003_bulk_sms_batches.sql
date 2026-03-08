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

alter table public.recall_attempts
    add column if not exists bulk_sms_batch_id uuid references public.bulk_sms_batches(id) on delete set null;

create index if not exists idx_bulk_sms_batches_surgery_created
    on public.bulk_sms_batches (surgery_id, created_at desc);

create index if not exists idx_recall_attempts_bulk_sms_batch
    on public.recall_attempts (bulk_sms_batch_id);

drop trigger if exists trg_bulk_sms_batches_updated_at on public.bulk_sms_batches;
create trigger trg_bulk_sms_batches_updated_at
before update on public.bulk_sms_batches
for each row execute function public.set_timestamp();

alter table public.bulk_sms_batches enable row level security;

drop policy if exists bulk_sms_batches_tenant on public.bulk_sms_batches;
create policy bulk_sms_batches_tenant on public.bulk_sms_batches
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);
