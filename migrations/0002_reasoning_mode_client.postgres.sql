-- reasoning_mode may now be "client": the effort is chosen by the caller per
-- request instead of being fixed by the gateway.
ALTER TABLE model_call DROP CONSTRAINT IF EXISTS model_call_reasoning_mode_check;

ALTER TABLE model_call ADD CONSTRAINT model_call_reasoning_mode_check
    CHECK (reasoning_mode IN ('min','medium','high','max','client'));
