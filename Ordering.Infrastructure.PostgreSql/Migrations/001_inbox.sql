CREATE TABLE public.ordering_inbox (
    inbox_id uuid PRIMARY KEY,
    partner_id varchar(200) COLLATE "C" NOT NULL CHECK (length(btrim(partner_id)) > 0),
    integration_scope varchar(200) COLLATE "C" NOT NULL CHECK (length(btrim(integration_scope)) > 0),
    event_id varchar(200) COLLATE "C" NOT NULL CHECK (length(btrim(event_id)) > 0),
    order_id varchar(200) COLLATE "C" NOT NULL CHECK (length(btrim(order_id)) > 0),
    original_payload bytea NOT NULL CHECK (octet_length(original_payload) BETWEEN 1 AND 1048576),
    normalized_payload jsonb NOT NULL CHECK (jsonb_typeof(normalized_payload) = 'object'),
    normalized_version integer NOT NULL CHECK (normalized_version = 1),
    received_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
    state text NOT NULL DEFAULT 'Pending' CHECK (state = 'Pending'),
    CONSTRAINT ordering_inbox_event_unique UNIQUE (partner_id, integration_scope, event_id),
    CONSTRAINT ordering_inbox_normalized_consistent CHECK (
        normalized_payload = jsonb_build_object('EventId', event_id, 'OrderId', order_id))
);
CREATE INDEX ordering_inbox_pending_received ON public.ordering_inbox (state, received_at_utc, inbox_id);
