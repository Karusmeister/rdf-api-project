-- SCHEMA_DEDUPE_PLAN #4: JSON -> JSONB on columns we keep.
--
-- Skips krs_entity_versions.raw on purpose — that table is dropped in
-- dedupe/003. Rewriting 349 MB only to drop it later is pure waste.
--
-- Idempotent: only touches columns still typed as json. Safe to re-run
-- even though schema_migrations already guards single-apply.
--
-- Drop the one dependent view that blocks ALTER COLUMN TYPE
-- (raw_financial_data.data_json → latest_raw_financial_data). The app's
-- _init_schema() in prediction_db.py recreates it via CREATE OR REPLACE
-- VIEW immediately after migrations run, so there is no observable gap.
DROP VIEW IF EXISTS latest_raw_financial_data;

DO $$
DECLARE
    targets text[][] := ARRAY[
        ['raw_financial_data',  'data_json'],
        ['predictions',         'feature_contributions'],
        ['predictions',         'feature_snapshot'],
        ['model_registry',      'hyperparameters'],
        ['model_registry',      'training_data_spec'],
        ['model_registry',      'training_metrics'],
        ['prediction_runs',     'parameters'],
        ['feature_definitions', 'required_tags'],
        ['assessment_jobs',     'result_json']
    ];
    t text;
    c text;
    cur_type text;
    i int;
BEGIN
    FOR i IN 1 .. array_length(targets, 1) LOOP
        t := targets[i][1];
        c := targets[i][2];
        SELECT data_type INTO cur_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = t AND column_name = c;
        IF cur_type = 'json' THEN
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN %I TYPE jsonb USING %I::jsonb',
                t, c, c
            );
        END IF;
    END LOOP;
END $$;
