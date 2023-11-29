-- Solution part --
DROP PROCEDURE IF EXISTS DropTablesStartsWith;
DROP PROCEDURE IF EXISTS UserFunctionSignatures;
DROP PROCEDURE IF EXISTS DropDMLTriggers;
DROP PROCEDURE IF EXISTS RoutineNames;

-- 1 --
CREATE OR REPLACE PROCEDURE DropTablesStartsWith(_name varchar(40))
AS $$ 
DECLARE
	_tableName varchar(255);
BEGIN 
	FOR _tableName IN
		select table_name from information_schema.tables
		where table_schema = 'public' and table_name like _name || '%'
	LOOP
		EXECUTE 'DROP TABLE IF EXISTS ' || _tableName || ' CASCADE';
	END LOOP;
END $$ LANGUAGE plpgsql;

-- 2 --
CREATE OR REPLACE PROCEDURE UserFunctionSignatures(OUT signatures text, OUT functionsCount int)
AS $proc$ BEGIN
	functionsCount = (
		select count(*)
		from (
			select distinct rout.routine_name
			from information_schema.parameters attr 
			join information_schema.routines rout 
				on rout.specific_schema = 'public' 
				and attr.specific_name = rout.specific_name
			where 
				rout.type_udt_name is not null and 
				rout.type_udt_name not in ('record', 'trigger') and 
				attr.parameter_mode = 'IN'
				) rout);
	
	signatures = (select string_agg(rout.routine_name || ' ' || attr.parameters || ' ' || rout.type_udt_name, ', ')
	from (
			select
				specific_name
				, '(' || string_agg(parameter_name || ' ' || udt_name, ', ') || ')' as parameters
			from information_schema.parameters 
			where specific_schema = 'public' and parameter_mode = 'IN'
			group by specific_name
			order by specific_name
	) attr join information_schema.routines rout on attr.specific_name = rout.specific_name
	where type_udt_name is not null and type_udt_name not in ('record', 'trigger'));
END $proc$ LANGUAGE plpgsql;

-- 3 --
CREATE OR REPLACE PROCEDURE DropDMLTriggers(OUT droppedTriggerCount int)
AS $$
DECLARE 
	_trigger RECORD;
BEGIN
	droppedTriggerCount = (select count(trigger_name) from information_schema.triggers
							where trigger_schema = 'public'
							and event_manipulation in ('INSERT', 'UPDATE', 'DELETE'));
	FOR _trigger IN
		select trigger_name, event_object_table from information_schema.triggers
		where trigger_schema = 'public'
		and event_manipulation in ('INSERT', 'UPDATE', 'DELETE')
	LOOP
		EXECUTE 'DROP TRIGGER IF EXISTS ' || _trigger.trigger_name || ' ON ' || _trigger.event_object_table || ' CASCADE';
	END LOOP;
END $$ LANGUAGE plpgsql;

-- 4 --
CREATE OR REPLACE PROCEDURE RoutineNames(
	OUT routines_string text, 
	OUT routines_count int,
	IN routine_definition_segment text DEFAULT '')
AS $proc$ 
DECLARE
	_routines record;
BEGIN
	select  
		string_agg(routine_name || ' ' || routine_type, ', ') as _string, 
		count(*) as _count
	into _routines
	from information_schema.routines
	where specific_schema = 'public'
	and routine_body = 'SQL'
	and (routine_type = 'FUNCTION' and type_udt_name not in ('record', 'trigger') and type_udt_name is not null
		 or routine_type = 'PROCEDURE')
	and routine_definition like '%' || routine_definition_segment || '%';
	routines_string = _routines._string;
	routines_count = _routines._count;
	DROP TABLE IF EXISTS _routines;
END $proc$ LANGUAGE plpgsql;


-- Test database --
DROP TABLE IF EXISTS Statistics;
DROP TABLE IF EXISTS StatisticsDict;
DROP TABLE IF EXISTS Deviations;
DROP TABLE IF EXISTS Observations;
DROP TABLE IF EXISTS Tests;

DROP FUNCTION IF EXISTS GetStatAggregatorName;
DROP FUNCTION IF EXISTS GetTestStat;
DROP FUNCTION IF EXISTS GetTestStatistics;
DROP FUNCTION IF EXISTS GetTestObservationsCount;

DROP PROCEDURE IF EXISTS InitDictionaries;
DROP PROCEDURE IF EXISTS AddTestObservation;
DROP PROCEDURE IF EXISTS UpdateTestStat;

DROP TRIGGER IF EXISTS trg_tests_after_insert ON Tests;
DROP FUNCTION IF EXISTS trg_fnc_FillTestStatistics;

DROP TRIGGER IF EXISTS trg_observations_after_insert ON Observations;
DROP FUNCTION IF EXISTS trg_fnc_UpdateTestStatistics;

DROP TRIGGER IF EXISTS trg_observations_before_insert ON Observations;
DROP FUNCTION IF EXISTS trg_fnc_InsertOrMoveToDeviation;

-- Tables --
CREATE TABLE IF NOT EXISTS Tests
(
	Id SERIAL PRIMARY KEY,
	Name VARCHAR(40) UNIQUE,
	Date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS Observations
(
	Id SERIAL PRIMARY KEY,
	TestId INT REFERENCES Tests(Id),
	Value REAL,
	Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Deviations
(
	Id SERIAL PRIMARY KEY,
	TestId INT REFERENCES Tests(Id),
	Value REAL,
	Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS StatisticsDict
(
	Id SERIAL PRIMARY KEY,
	Name VARCHAR(40) UNIQUE,
	DefaultValue REAL DEFAULT 0,
	AggregatorName VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS Statistics
(
	TestId INT REFERENCES Tests(Id),
	StatId INT REFERENCES StatisticsDict(Id),
	Value REAL,
	PRIMARY KEY(TestId, StatId)
);

-- Functions --
CREATE OR REPLACE FUNCTION GetStatAggregatorName(_statId INT) RETURNS VARCHAR(40)
	AS $$ (select AggregatorName from StatisticsDict where Id = _statId) $$
	LANGUAGE SQL;

CREATE OR REPLACE FUNCTION GetTestStat(_testId INT, _statId INT) RETURNS REAL
	AS $$ (select Value from Statistics stat where stat.TestId = _testId and stat.StatId = _statId) $$
	LANGUAGE SQL;

CREATE OR REPLACE FUNCTION GetTestStatistics(_testId INT) 
	RETURNS TABLE (Name VARCHAR(40), Value REAL)
	AS $$ (select Name, Value from Statistics stat join StatisticsDict dict on stat.StatId = dict.Id where stat.TestId = _testId) $$
	LANGUAGE SQL;
	
CREATE OR REPLACE FUNCTION GetTestObservationsCount(_testId INT) RETURNS INT
	AS $$ (select count(*) from Observations obsv where obsv.TestId = _testId) $$
	LANGUAGE SQL;

-- Procedures --
CREATE OR REPLACE PROCEDURE InitDictionaries() AS $$
BEGIN
	-- Statistics --
	INSERT INTO StatisticsDict(Name, AggregatorName) VALUES
		('avg', 'avg'),
		('var', 'var_pop'),
		('std', 'stddev_pop')
	;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE AddTestObservation(
	test VARCHAR(40), 
	observationValue REAL, 
	OUT _testId INT, OUT _observationId INT) AS $$ 
BEGIN
	INSERT INTO Tests (Name) VALUES (test) ON CONFLICT (Name) DO NOTHING;
		select Id from Tests where Name = test into _testId;
	INSERT INTO Observations (TestId, Value) VALUES (_testId, observationValue)
		RETURNING Id INTO _observationId;
END 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE UpdateTestStat(_testId INT, _statId INT) AS $$
BEGIN
	EXECUTE 'UPDATE Statistics SET Value = (SELECT ' || (select * from GetStatAggregatorName(_statId)) || '(Value) from Observations where TestId = ' || _testId || ') WHERE TestId = ' || _testId || ' AND StatId = ' || _statId;
END
$$ LANGUAGE Plpgsql;

-- Triggers --
CREATE OR REPLACE FUNCTION trg_fnc_FillTestStatistics() RETURNS TRIGGER AS $Tests$
BEGIN
	INSERT INTO Statistics (TestId, StatId, Value) 
	SELECT NEW.Id, Stat.Id, stat.DefaultValue FROM StatisticsDict stat;
	RETURN NEW;
END
$Tests$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_tests_after_insert
	AFTER INSERT ON Tests
	FOR EACH ROW EXECUTE FUNCTION trg_fnc_FillTestStatistics();
	
CREATE OR REPLACE FUNCTION trg_fnc_UpdateTestStatistics() RETURNS TRIGGER AS $Observations$
DECLARE
	_statId INT;
BEGIN
	FOR _statId IN
		select Id from StatisticsDict
	LOOP
		CALL UpdateTestStat(NEW.TestId, _statId);
	END LOOP;
	RETURN NEW;
END
$Observations$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_observation_after_insert
	AFTER INSERT ON Observations
	FOR EACH ROW EXECUTE FUNCTION trg_fnc_UpdateTestStatistics();
	
CREATE OR REPLACE FUNCTION trg_fnc_InsertOrMoveToDeviation() RETURNS TRIGGER AS $Observations$
BEGIN
	IF GetTestObservationsCount(NEW.TestId) > 3 
	-- ABS(value - avg) > 3 * std
	AND ABS(NEW.Value - GetTestStat(NEW.TestId, 1)) > 3 * GetTestStat(New.TestId, 3) 
	THEN
		INSERT INTO Deviations (TestId, Value) VALUES (NEW.TestId, NEW.Value);
		RETURN NULL;
	END IF;
	RETURN NEW;
END
$Observations$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_observation_before_insert
	BEFORE INSERT ON Observations
	FOR EACH ROW EXECUTE FUNCTION trg_fnc_InsertOrMoveToDeviation();
	
-- Tests --
CREATE SCHEMA IF NOT EXISTS test 
	AUTHORIZATION pg_database_owner;
GRANT USAGE ON SCHEMA test TO PUBLIC;
GRANT ALL ON SCHEMA test TO pg_database_owner;

DROP PROCEDURE IF EXISTS test.Test__1;
DROP PROCEDURE IF EXISTS test.Test__2;
DROP PROCEDURE IF EXISTS test.Test__3;
DROP PROCEDURE IF EXISTS test.Test__4;
DROP PROCEDURE IF EXISTS test.Test__part4;

CREATE OR REPLACE PROCEDURE test.Test__1()
AS $$ 
DECLARE
	_tableCount INT;
BEGIN
	select count(table_name) into _tableCount
	from information_schema.tables
	where table_schema = 'public';
	ASSERT _tableCount = 5;
	
	CALL DropTablesStartsWith('s');
	select count(table_name) into _tableCount
	from information_schema.tables where table_schema = 'public';
	ASSERT _tableCount = 3;
	
	CALL DropTablesStartsWith('o');
	select count(table_name) into _tableCount
	from information_schema.tables where table_schema = 'public';
	ASSERT _tableCount = 2;
	
	CALL DropTablesStartsWith('deviation');
	select count(table_name) into _tableCount
	from information_schema.tables where table_schema = 'public';
	ASSERT _tableCount = 1;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE test.Test__2()
AS $$ 
DECLARE
	_functionCount INT;
	_signatures text;
BEGIN
	CALL UserFunctionSignatures(_signatures, _functionCount);
	ASSERT _functionCount = 3;
	ASSERT _signatures = 'getstataggregatorname (_statid int4) varchar, getteststat (_testid int4, _statid int4) float4, gettestobservationscount (_testid int4) int4';
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE test.Test__3()
AS $$ 
DECLARE
	_triggerCount INT;
BEGIN
	_triggerCount = (select count(trigger_name) from information_schema.triggers
							where trigger_schema = 'public'
							and event_manipulation in ('INSERT', 'UPDATE', 'DELETE'));
	ASSERT _triggerCount = 3;
	
	CALL DropDMLTriggers(_triggerCount);
	ASSERT _triggerCount = 3; -- deleted
	
	_triggerCount = (select count(trigger_name) from information_schema.triggers
							where trigger_schema = 'public'
							and event_manipulation in ('INSERT', 'UPDATE', 'DELETE'));
	ASSERT _triggerCount = 0;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE test.Test__4()
AS $$
DECLARE
	_routines TEXT;
	_routinesCount INT;
BEGIN
	CALL RoutineNames(_routines, _routinesCount, 'select');
	ASSERT _routinesCount = 3;
	ASSERT _routines = 'getstataggregatorname FUNCTION, getteststat FUNCTION, gettestobservationscount FUNCTION';
	
	CALL RoutineNames(_routines, _routinesCount, 'AggregatorName');
	ASSERT _routinesCount = 1;
	ASSERT _routines = 'getstataggregatorname FUNCTION';
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE test.Test__part4()
AS $$
BEGIN
	CALL test.Test__4();
	CALL test.Test__3();
	CALL test.Test__2();
	CALL test.Test__1();
END
$$ LANGUAGE plpgsql;

CALL test.Test__part4();
