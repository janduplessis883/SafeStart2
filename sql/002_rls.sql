alter table public.surgeries enable row level security;
alter table public.surgery_users enable row level security;
alter table public.import_batches enable row level security;
alter table public.patients enable row level security;
alter table public.import_rows enable row level security;
alter table public.vaccination_events enable row level security;
alter table public.recall_recommendations enable row level security;
alter table public.recall_attempts enable row level security;
alter table public.bulk_sms_batches enable row level security;
alter table public.recall_batches enable row level security;
alter table public.vaccine_alias_overrides enable row level security;

create or replace function public.current_user_email()
returns text
language sql
stable
as $$
    select lower(coalesce(auth.jwt() ->> 'email', ''))
$$;

create or replace function public.current_surgery_id()
returns uuid
language sql
stable
security definer
set search_path = public
as $$
    select su.surgery_id
    from public.surgery_users su
    where lower(su.email) = public.current_user_email()
      and su.is_active = true
    limit 1
$$;

create or replace function public.current_user_role()
returns text
language sql
stable
security definer
set search_path = public
as $$
    select su.role
    from public.surgery_users su
    where lower(su.email) = public.current_user_email()
      and su.is_active = true
    limit 1
$$;

create or replace function public.is_superuser()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
    select coalesce(public.current_user_role() = 'superuser', false)
$$;

drop policy if exists surgeries_select on public.surgeries;
create policy surgeries_select on public.surgeries
for select using (
    public.is_superuser()
    or id = public.current_surgery_id()
);

drop policy if exists surgeries_mutate on public.surgeries;
create policy surgeries_mutate on public.surgeries
for all using (public.is_superuser())
with check (public.is_superuser());

drop policy if exists surgery_users_select on public.surgery_users;
create policy surgery_users_select on public.surgery_users
for select using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists surgery_users_mutate on public.surgery_users;
create policy surgery_users_mutate on public.surgery_users
for all using (public.is_superuser())
with check (public.is_superuser());

drop policy if exists import_batches_tenant on public.import_batches;
create policy import_batches_tenant on public.import_batches
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists patients_tenant on public.patients;
create policy patients_tenant on public.patients
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists import_rows_tenant on public.import_rows;
create policy import_rows_tenant on public.import_rows
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists vaccination_events_tenant on public.vaccination_events;
create policy vaccination_events_tenant on public.vaccination_events
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists recall_recommendations_tenant on public.recall_recommendations;
create policy recall_recommendations_tenant on public.recall_recommendations
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists recall_attempts_tenant on public.recall_attempts;
create policy recall_attempts_tenant on public.recall_attempts
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

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

drop policy if exists recall_batches_tenant on public.recall_batches;
create policy recall_batches_tenant on public.recall_batches
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);

drop policy if exists vaccine_alias_overrides_tenant on public.vaccine_alias_overrides;
create policy vaccine_alias_overrides_tenant on public.vaccine_alias_overrides
for select using (
    public.is_superuser()
    or surgery_id is null
    or surgery_id = public.current_surgery_id()
);

drop policy if exists vaccine_alias_overrides_mutate on public.vaccine_alias_overrides;
create policy vaccine_alias_overrides_mutate on public.vaccine_alias_overrides
for all using (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
)
with check (
    public.is_superuser()
    or surgery_id = public.current_surgery_id()
);
