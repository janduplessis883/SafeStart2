alter table public.patients
add column if not exists email text;

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
