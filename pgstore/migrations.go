package pgstore

import "fmt"

// buildDDL returns SQL statements that create the liveconfig tables, index,
// notify function, and trigger.
//
// Table and channel names are parameterised so that multiple liveconfig
// instances can coexist in the same database (e.g. one per environment).
//
// This SQL is idempotent: running it multiple times is safe.
func buildDDL(valuesTable, auditTable, channel string) string {
	// Constraint, index, function, and trigger names embed the table name so
	// they are unique when multiple table names are configured.
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
			id          BIGSERIAL    PRIMARY KEY,
			namespace   TEXT         NOT NULL,
			key         TEXT         NOT NULL,
			value       TEXT         NOT NULL DEFAULT '',
			changed_by  TEXT         NOT NULL DEFAULT '',
			updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			CONSTRAINT  %[1]s_ns_key_uq UNIQUE (namespace, key)
		);

		CREATE TABLE IF NOT EXISTS %[2]s (
			id          BIGSERIAL    PRIMARY KEY,
			namespace   TEXT         NOT NULL,
			key         TEXT         NOT NULL,
			old_value   TEXT         NOT NULL DEFAULT '',
			new_value   TEXT         NOT NULL DEFAULT '',
			changed_by  TEXT         NOT NULL DEFAULT '',
			changed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS %[2]s_ns_key_idx
			ON %[2]s (namespace, key, id DESC);

		CREATE OR REPLACE FUNCTION liveconfig_notify_fn_%[1]s()
		RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('%[3]s', json_build_object(
				'namespace',  NEW.namespace,
				'key',        NEW.key,
				'old_value',  COALESCE(OLD.value, ''),
				'new_value',  NEW.value,
				'changed_by', NEW.changed_by,
				'changed_at', NOW()
			)::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		DROP TRIGGER IF EXISTS liveconfig_notify_trg_%[1]s ON %[1]s;
		CREATE TRIGGER liveconfig_notify_trg_%[1]s
			AFTER INSERT OR UPDATE ON %[1]s
			FOR EACH ROW EXECUTE FUNCTION liveconfig_notify_fn_%[1]s();
	`, valuesTable, auditTable, channel)
}
