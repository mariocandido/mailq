CREATE TABLE IF NOT EXISTS messages (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT,
    sender          TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    domain          TEXT NOT NULL,
    subject         TEXT NOT NULL,
    body            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Same Idempotency-Key + recipient never creates a second message.
CREATE UNIQUE INDEX IF NOT EXISTS messages_idempotency
    ON messages (idempotency_key, recipient) WHERE idempotency_key IS NOT NULL;

-- Workers only ever scan messages that are waiting.
CREATE INDEX IF NOT EXISTS messages_ready
    ON messages (next_attempt_at) WHERE status IN ('queued', 'deferred');

-- Leases that need to be recovered after a worker crash.
CREATE INDEX IF NOT EXISTS messages_leased
    ON messages (locked_until) WHERE status = 'sending';

CREATE TABLE IF NOT EXISTS attempts (
    id          BIGSERIAL PRIMARY KEY,
    message_id  UUID NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
    at          TIMESTAMPTZ NOT NULL,
    host        TEXT NOT NULL,
    code        INT NOT NULL,
    enhanced    TEXT NOT NULL,
    response    TEXT NOT NULL,
    class       TEXT NOT NULL,
    reason      TEXT NOT NULL,
    duration_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS attempts_message ON attempts (message_id, at);

CREATE TABLE IF NOT EXISTS suppressions (
    recipient  TEXT PRIMARY KEY,
    reason     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
