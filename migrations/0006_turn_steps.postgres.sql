ALTER TABLE dialogue ADD COLUMN turn_steps BIGINT NOT NULL DEFAULT 0 CHECK (turn_steps >= 0);
