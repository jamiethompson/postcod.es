BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'stage'
          AND table_name = 'streets_usrn_input'
          AND column_name = 'street_class'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'stage'
          AND table_name = 'streets_usrn_input'
          AND column_name = 'street_type'
    ) THEN
        ALTER TABLE stage.streets_usrn_input
            RENAME COLUMN street_class TO street_type;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'core'
          AND table_name = 'streets_usrn'
          AND column_name = 'street_class'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'core'
          AND table_name = 'streets_usrn'
          AND column_name = 'street_type'
    ) THEN
        ALTER TABLE core.streets_usrn
            RENAME COLUMN street_class TO street_type;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'stage'
          AND table_name = 'streets_usrn_input'
          AND column_name = 'street_name'
          AND is_nullable = 'NO'
    ) THEN
        ALTER TABLE stage.streets_usrn_input
            ALTER COLUMN street_name DROP NOT NULL;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'stage'
          AND table_name = 'streets_usrn_input'
          AND column_name = 'street_name_casefolded'
          AND is_nullable = 'NO'
    ) THEN
        ALTER TABLE stage.streets_usrn_input
            ALTER COLUMN street_name_casefolded DROP NOT NULL;
    END IF;
END $$;

COMMIT;
