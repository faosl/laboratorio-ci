ALTER TABLE public.ordering_inbox
    DROP CONSTRAINT ordering_inbox_state_check,
    ADD COLUMN lease_owner varchar(200) COLLATE "C",
    ADD COLUMN lease_expires_at_utc timestamptz,
    ADD COLUMN lease_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at_utc timestamptz,
    ADD CONSTRAINT ordering_inbox_state_check
        CHECK (state IN ('Pending', 'Processing', 'RetryScheduled', 'Completed')),
    ADD CONSTRAINT ordering_inbox_generation_check CHECK (lease_generation >= 0),
    ADD CONSTRAINT ordering_inbox_lease_check CHECK (
        (state = 'Processing' AND lease_owner IS NOT NULL
            AND length(btrim(lease_owner)) > 0 AND lease_expires_at_utc IS NOT NULL
            AND isfinite(lease_expires_at_utc) AND lease_generation > 0)
        OR (state <> 'Processing' AND lease_owner IS NULL AND lease_expires_at_utc IS NULL)),
    ADD CONSTRAINT ordering_inbox_retry_check CHECK (
        (state = 'RetryScheduled' AND next_attempt_at_utc IS NOT NULL AND isfinite(next_attempt_at_utc))
        OR (state <> 'RetryScheduled' AND next_attempt_at_utc IS NULL));

-- One ordered access path matches the acquisition query across all three eligible states.
-- Pending uses receipt time; retries and recovery use their respective due times.
DROP INDEX public.ordering_inbox_pending_received;
CREATE INDEX ordering_inbox_processing_due ON public.ordering_inbox
    ((CASE state WHEN 'Pending' THEN received_at_utc
        WHEN 'RetryScheduled' THEN next_attempt_at_utc ELSE lease_expires_at_utc END),
     received_at_utc, inbox_id)
    WHERE state IN ('Pending', 'RetryScheduled', 'Processing');
