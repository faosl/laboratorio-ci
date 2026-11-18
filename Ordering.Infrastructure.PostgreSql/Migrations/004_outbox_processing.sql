ALTER TABLE public.ordering_outbox_messages
    DROP CONSTRAINT ordering_outbox_messages_publication_status_check,
    ADD COLUMN lease_owner varchar(200) COLLATE "C",
    ADD COLUMN lease_expires_at_utc timestamptz,
    ADD COLUMN lease_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at_utc timestamptz,
    ADD COLUMN published_at_utc timestamptz,
    ADD CONSTRAINT ordering_outbox_status_check
        CHECK (publication_status IN ('Pending', 'Processing', 'RetryScheduled', 'Published')),
    ADD CONSTRAINT ordering_outbox_generation_check CHECK (
        (publication_status = 'Pending' AND lease_generation = 0)
        OR (publication_status <> 'Pending' AND lease_generation > 0)),
    ADD CONSTRAINT ordering_outbox_lease_check CHECK (
        (publication_status = 'Processing' AND lease_owner IS NOT NULL
            AND length(btrim(lease_owner)) > 0 AND lease_expires_at_utc IS NOT NULL
            AND isfinite(lease_expires_at_utc))
        OR (publication_status <> 'Processing' AND lease_owner IS NULL AND lease_expires_at_utc IS NULL)),
    ADD CONSTRAINT ordering_outbox_retry_check CHECK (
        (publication_status = 'RetryScheduled' AND next_attempt_at_utc IS NOT NULL AND isfinite(next_attempt_at_utc))
        OR (publication_status <> 'RetryScheduled' AND next_attempt_at_utc IS NULL)),
    ADD CONSTRAINT ordering_outbox_published_check CHECK (
        (publication_status = 'Published' AND published_at_utc IS NOT NULL AND isfinite(published_at_utc))
        OR (publication_status <> 'Published' AND published_at_utc IS NULL));

-- Work claiming shares one due-time access path; SKIP LOCKED does not guarantee strict FIFO.
CREATE INDEX ordering_outbox_processing_due ON public.ordering_outbox_messages
    ((CASE publication_status WHEN 'Pending' THEN '-infinity'::timestamptz
        WHEN 'RetryScheduled' THEN next_attempt_at_utc ELSE lease_expires_at_utc END),
     created_at_utc, event_id)
    WHERE publication_status IN ('Pending', 'RetryScheduled', 'Processing');
