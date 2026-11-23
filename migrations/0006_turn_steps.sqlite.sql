ALTER TABLE dialogue ADD COLUMN turn_steps INTEGER NOT NULL DEFAULT 0 CHECK (turn_steps >= 0);
