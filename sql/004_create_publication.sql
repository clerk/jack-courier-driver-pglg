DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '{publication_name}') THEN
        EXECUTE format(
            'CREATE PUBLICATION %I FOR TABLE %I.%I WITH (publish = ''insert'')',
            '{publication_name}', '{schema}', '{prefix}_jobs'
        );
    END IF;
END $$;
