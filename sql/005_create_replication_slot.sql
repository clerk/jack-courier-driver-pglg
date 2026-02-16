DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}') THEN
        PERFORM pg_create_logical_replication_slot('{slot_name}', 'pgoutput');
    END IF;
END $$;
