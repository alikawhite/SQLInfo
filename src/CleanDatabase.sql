DROP PROCEDURE IF EXISTS DropAllTables;
DROP PROCEDURE IF EXISTS DropAllProcedures;
DROP PROCEDURE IF EXISTS DropAllFunctions;
DROP PROCEDURE IF EXISTS DropAllTriggers;
DROP PROCEDURE IF EXISTS CleanDatabase;

CREATE OR REPLACE PROCEDURE DropAllTables() AS
$$
DECLARE
    _objectName VARCHAR(255);
BEGIN
    FOR _objectName IN
        SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || _objectName || ' CASCADE';
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE DropAllProcedures() AS
$$
DECLARE
    _objectName VARCHAR(255);
BEGIN
    FOR _objectName IN
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_schema = 'public' AND routine_type = 'PROCEDURE'
        LOOP
            EXECUTE 'DROP PROCEDURE IF EXISTS ' || _objectName;
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE DropAllFunctions() AS
$$
DECLARE
    _objectName VARCHAR(255);
BEGIN
    FOR _objectName IN
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_schema = 'public' AND routine_type = 'FUNCTION'
        LOOP
            EXECUTE 'DROP FUNCTION IF EXISTS ' || _objectName;
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE DropAllTriggers() AS
$$
DECLARE
    _trigger RECORD;
BEGIN
    FOR _trigger IN
        SELECT trigger_name AS name, event_object_table AS table
        FROM information_schema.triggers
        WHERE trigger_schema = 'public'
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || _trigger.name || ' ON ' || _trigger.table;
        END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE DropTestSchema() AS
$$
DECLARE
    _trigger RECORD;
BEGIN
    DROP SCHEMA IF EXISTS test CASCADE;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE CleanDatabase() AS
$$
BEGIN
    CALL DropTestSchema();
    CALL DropAllTables();
    CALL DropAllTriggers();
    CALL DropAllFunctions();
    CALL DropAllProcedures();

END
$$ LANGUAGE plpgsql;

CALL CleanDatabase();
