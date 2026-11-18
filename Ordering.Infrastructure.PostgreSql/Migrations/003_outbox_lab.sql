-- Laboratory business state is independent of Domain.Order and Inbox processing state.
CREATE TABLE public.ordering_lab_orders (
    order_id varchar(200) COLLATE "C" PRIMARY KEY CHECK (length(btrim(order_id)) > 0),
    status text NOT NULL DEFAULT 'Pending' CHECK (status IN ('Pending', 'Confirmed'))
);

CREATE TABLE public.ordering_outbox_messages (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL CHECK (event_type = 'OrderConfirmed'),
    aggregate_id varchar(200) COLLATE "C" NOT NULL REFERENCES public.ordering_lab_orders (order_id),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    publication_status text NOT NULL DEFAULT 'Pending' CHECK (publication_status = 'Pending'),
    created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp()
);
