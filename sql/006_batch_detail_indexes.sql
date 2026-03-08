create index if not exists idx_vaccination_events_batch
    on public.vaccination_events (batch_id);

create index if not exists idx_recall_recommendations_batch
    on public.recall_recommendations (batch_id);
