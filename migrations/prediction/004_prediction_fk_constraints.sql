-- 004: enforce relational integrity for prediction tables (CR2-DB-003).
--
-- Adds three foreign keys that should have existed from day one:
--   * prediction_runs.model_id          → model_registry(id)     ON DELETE RESTRICT
--   * predictions.prediction_run_id     → prediction_runs(id)    ON DELETE CASCADE
--   * predictions.report_id             → financial_reports(id)  ON DELETE RESTRICT
--
-- Policy rationale:
--   - RESTRICT on model_id/report_id: deleting a model or report that still
--     has predictions almost certainly means a data-loss bug; fail loud and
--     force operators to clean up explicitly.
--   - CASCADE on prediction_run_id: a prediction only makes sense in the
--     context of its run, so dropping a run should carry its children with
--     it. This also makes re-scoring safe without manual cleanup.
--
-- Orphan handling:
--   This migration FAILS LOUD if orphans exist. We do NOT auto-delete — the
--   review explicitly requires a documented cleanup step, and silently
--   removing rows could destroy the only copy of a prediction on a
--   misconfigured environment. An operator sees the RAISE with row counts,
--   runs a cleanup script manually, and re-triggers the migration.
--
-- Idempotency:
--   Each ALTER TABLE is wrapped in a DO block that checks pg_constraint for
--   the FK name before attempting to add it, so this migration is safe to
--   apply on a DB that already has the constraints (e.g. a dev machine that
--   was patched out-of-band).

DO $$
DECLARE
    orphan_runs         INTEGER;
    orphan_predictions  INTEGER;
    orphan_reports      INTEGER;
BEGIN
    -- Count orphans before touching the schema.
    SELECT count(*) INTO orphan_runs
    FROM prediction_runs pr
    LEFT JOIN model_registry mr ON mr.id = pr.model_id
    WHERE mr.id IS NULL;

    SELECT count(*) INTO orphan_predictions
    FROM predictions p
    LEFT JOIN prediction_runs pr ON pr.id = p.prediction_run_id
    WHERE pr.id IS NULL;

    SELECT count(*) INTO orphan_reports
    FROM predictions p
    LEFT JOIN financial_reports fr ON fr.id = p.report_id
    WHERE fr.id IS NULL;

    IF orphan_runs > 0 OR orphan_predictions > 0 OR orphan_reports > 0 THEN
        RAISE EXCEPTION
            'CR2-DB-003 migration blocked by orphan rows: '
            'prediction_runs without a model_registry match=%, '
            'predictions without a prediction_run match=%, '
            'predictions without a financial_reports match=%. '
            'Resolve orphans (e.g. register missing models, delete dangling rows) '
            'and re-run migrations.',
            orphan_runs, orphan_predictions, orphan_reports;
    END IF;
END
$$;

-- prediction_runs.model_id → model_registry(id)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_prediction_runs_model_id'
    ) THEN
        ALTER TABLE prediction_runs
            ADD CONSTRAINT fk_prediction_runs_model_id
            FOREIGN KEY (model_id)
            REFERENCES model_registry(id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE;
    END IF;
END
$$;

-- predictions.prediction_run_id → prediction_runs(id)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_predictions_run_id'
    ) THEN
        ALTER TABLE predictions
            ADD CONSTRAINT fk_predictions_run_id
            FOREIGN KEY (prediction_run_id)
            REFERENCES prediction_runs(id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;
    END IF;
END
$$;

-- predictions.report_id → financial_reports(id)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_predictions_report_id'
    ) THEN
        ALTER TABLE predictions
            ADD CONSTRAINT fk_predictions_report_id
            FOREIGN KEY (report_id)
            REFERENCES financial_reports(id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE;
    END IF;
END
$$;
