CREATE TABLE events (
  id             BIGSERIAL PRIMARY KEY,
  event_id       BYTEA,
  event_type     TEXT,
  source_name    TEXT,
  event_time     TIMESTAMPTZ,
  severity       INT,
  message        TEXT,
  condition_name TEXT,
  active         BOOLEAN,
  acked          BOOLEAN,
  raw            JSONB,
  received_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX events_event_time_idx ON events (event_time);
CREATE INDEX events_event_type_idx ON events (event_type);
