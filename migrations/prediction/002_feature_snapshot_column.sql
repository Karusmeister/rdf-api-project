-- 002: immutable feature_snapshot on predictions (CR2-OPS-004).
--
-- Moved out of app/db/prediction_db.py `_init_schema`. Stores the exact
-- {feature_definition_id → computation_version} map captured at scoring time
-- so downstream reads can reconstruct the snapshot without timestamp-based
-- heuristics. Idempotent via IF NOT EXISTS.

ALTER TABLE predictions
    ADD COLUMN IF NOT EXISTS feature_snapshot JSON;
