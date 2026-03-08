alter table public.recall_recommendations
drop constraint if exists recall_recommendations_status_check;

alter table public.recall_recommendations
add constraint recall_recommendations_status_check
check (
    status in (
        'due_soon',
        'due_now',
        'overdue',
        'unvaccinated',
        'review',
        'complete',
        'suppressed',
        'resolved_by_vaccination'
    )
);
