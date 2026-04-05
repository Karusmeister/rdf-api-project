-- 003: ensure x1_maczynska.required_tags includes CF.A_II_1 (CR-006 / R2-003,
-- moved out of startup path by CR2-OPS-004).
--
-- The seed script is authoritative for feature_definitions, but environments
-- may not re-run it on every deploy. This migration patches existing rows
-- that predate the CF.A_II_1 fix; the seed script already produces the
-- correct shape for newly created rows.
--
-- Filtered on a text scan so it only touches rows where CF.A_II_1 is truly
-- missing from the JSON. The feature_definitions table may not yet exist on
-- fresh installs (seed hasn't run), hence the EXCEPTION catch.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'feature_definitions') THEN
        UPDATE feature_definitions
        SET required_tags = '["RZiS.I", "CF.A_II_1", "Pasywa_B"]'::json
        WHERE id = 'x1_maczynska'
          AND (required_tags IS NULL
               OR required_tags::text NOT LIKE '%CF.A_II_1%');
    END IF;
END
$$;
