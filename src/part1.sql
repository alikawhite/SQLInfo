DROP TABLE IF EXISTS Peers CASCADE;
DROP TABLE IF EXISTS Tasks CASCADE;
DROP TABLE IF EXISTS Checks CASCADE;
DROP TABLE IF EXISTS P2P CASCADE;
DROP TABLE IF EXISTS Verter CASCADE;
DROP TABLE IF EXISTS TransferredPoints CASCADE;
DROP TABLE IF EXISTS Friends CASCADE;
DROP TABLE IF EXISTS Recommendations CASCADE;
DROP TABLE IF EXISTS TimeTracking CASCADE;
DROP TABLE IF EXISTS XP CASCADE;

DROP PROCEDURE IF EXISTS export_table_to_csv;
DROP PROCEDURE IF EXISTS import_csv_to_table;

DROP TYPE IF EXISTS check_status CASCADE;
DROP TYPE IF EXISTS time_status CASCADE;

DROP SCHEMA IF EXISTS test CASCADE;

------------- CREATE TYPE -------------

CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');
CREATE TYPE time_status AS ENUM ('1', '2');

------------- CREATE TABLE -------------

CREATE TABLE IF NOT EXISTS Peers
(
    Nickname VARCHAR(40) PRIMARY KEY,
    Birthday DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS Tasks
(
    Title      VARCHAR(255) PRIMARY KEY,
    ParentTask VARCHAR(255) REFERENCES Tasks (Title) ON DELETE CASCADE,
    MaxXP      INT NOT NULL CHECK (MaxXP > 0)
);

CREATE TABLE IF NOT EXISTS Checks
(
    ID     SERIAL PRIMARY KEY,
    Peer   VARCHAR(40)  NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE,
    Task   VARCHAR(255) NOT NULL REFERENCES Tasks (Title) ON DELETE CASCADE,
    "Date" DATE         NOT NULL
);

CREATE TABLE IF NOT EXISTS P2P
(
    ID           SERIAL PRIMARY KEY,
    "Check"      INT          NOT NULL REFERENCES Checks (ID) ON DELETE CASCADE,
    CheckingPeer VARCHAR(40)  NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE,
    State        check_status NOT NULL,
    "Time"       TIME WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS Verter
(
    ID      SERIAL PRIMARY KEY,
    "Check" INT          NOT NULL REFERENCES Checks (ID) ON DELETE CASCADE,
    State   check_status NOT NULL,
    "Time"  TIME WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS TransferredPoints
(
    ID           SERIAL PRIMARY KEY,
    CheckingPeer VARCHAR(40) NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE,
    CheckedPeer  VARCHAR(40) NOT NULL CHECK (CheckingPeer != CheckedPeer) REFERENCES Peers (Nickname) ON DELETE CASCADE,
    PointsAmount INT
);

CREATE TABLE IF NOT EXISTS Friends
(
    ID    SERIAL PRIMARY KEY,
    Peer1 VARCHAR(40) NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE,
    Peer2 VARCHAR(40) NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Recommendations
(
    ID              SERIAL PRIMARY KEY,
    Peer            VARCHAR(40) NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE,
    RecommendedPeer VARCHAR(40) NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS XP
(
    ID       SERIAL PRIMARY KEY,
    "Check"  INT NOT NULL REFERENCES Checks (ID) ON DELETE CASCADE,
    XPAmount INT CHECK (XPAmount >= 0)
);

CREATE TABLE IF NOT EXISTS TimeTracking
(
    ID     SERIAL PRIMARY KEY,
    Peer   VARCHAR(40) NOT NULL REFERENCES Peers (Nickname) ON DELETE CASCADE,
    "Date" DATE,
    "Time" TIME WITHOUT TIME ZONE,
    State  INT CHECK (State IN (1, 2))
);

------------- ADD NEW RECORDS -------------

INSERT INTO Peers
VALUES ('aboba', '2001-04-19'),
       ('amogus', '1989-11-11'),
       ('bus', '1998-08-21'),
       ('lalala', '1990-12-27'),
       ('helpme', '1999-09-10'),
       ('iwannaeat', '2003-05-26'),
       ('testfunc', '10.03.2001');


INSERT INTO Tasks
VALUES ('C6_s21_matrix', NULL, 200),
       ('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
       ('C8_3DViewer_v1.0', 'C7_SmartCalc_v1.0', 750),
       ('CPP1_s21_matrix+', 'C8_3DViewer_v1.0', 300),
       ('DO1_Linux', 'C6_s21_matrix', 300),
       ('DO2_Linux_Network', 'DO1_Linux', 250);

INSERT INTO Checks (Peer, Task, "Date")
VALUES ('aboba', 'C6_s21_matrix', '2023-06-27'),
       ('amogus', 'C6_s21_matrix', '2023-06-27'),
       ('bus', 'C6_s21_matrix', '2023-06-27'),
       ('helpme', 'C6_s21_matrix', '2023-06-27'),
       ('lalala', 'C6_s21_matrix', '2023-06-27'),
       ('aboba', 'C6_s21_matrix', '2023-06-27'),
       ('aboba', 'C6_s21_matrix', '2023-06-27'),
       ('aboba', 'C6_s21_matrix', '2023-06-27'),
       ('amogus', 'DO1_Linux', '2023-06-29'),
       ('amogus', 'DO1_Linux', '2023-06-29'),
       ('amogus', 'DO1_Linux', '2023-06-29');

INSERT INTO P2P ("Check", CheckingPeer, State, "Time")
VALUES (1, 'iwannaeat', 'Start', '12:02'),
       (1, 'iwannaeat', 'Success', '12:24'),
       (2, 'iwannaeat', 'Start', '13:02'),
       (2, 'iwannaeat', 'Success', '13:24'),
       (3, 'amogus', 'Start', '14:02'),
       (3, 'amogus', 'Success', '14:24'),
       (4, 'bus', 'Start', '14:29'),
       (4, 'bus', 'Success', '14:32'),
       (5, 'aboba', 'Start', '15:02'),
       (5, 'aboba', 'Failure', '15:23'),
       (6, 'helpme', 'Start', '15:45'),
       (6, 'helpme', 'Success', '15:59'),
       (7, 'amogus', 'Start', '16:00'),
       (7, 'amogus', 'Success', '16:23'),
       (8, 'iwannaeat', 'Start', '12:02'),
       (8, 'iwannaeat', 'Success', '12:24'),
       (9, 'aboba', 'Start', '13:02'),
       (9, 'aboba', 'Success', '13:24'),
       (10, 'bus', 'Start', '14:02'),
       (10, 'bus', 'Success', '14:24');

INSERT INTO Verter ("Check", State, "Time")
VALUES (8, 'Start', '17:20'),
       (8, 'Success', '17:23');

INSERT INTO TransferredPoints (CheckingPeer, CheckedPeer, PointsAmount)
VALUES ('aboba', 'iwannaeat', 1),
       ('amogus', 'iwannaeat', 1),
       ('bus', 'amogus', 1),
       ('helpme', 'bus', 1),
       ('lalala', 'aboba', 1),
       ('aboba', 'helpme', 1),
       ('aboba', 'amogus', 1);

INSERT INTO Friends (Peer1, Peer2)
VALUES ('amogus', 'aboba'),
       ('amogus', 'bus'),
       ('lalala', 'helpme'),
       ('helpme', 'iwannaeat'),
       ('iwannaeat', 'lalala');

INSERT INTO Recommendations (Peer, RecommendedPeer)
VALUES ('lalala', 'aboba'),
       ('aboba', 'iwannaeat'),
       ('amogus', 'iwannaeat'),
       ('aboba', 'amogus'),
       ('aboba', 'helpme');

INSERT INTO XP ("Check", XPAmount)
VALUES (5, 0),
       (8, 180);

INSERT INTO TimeTracking (Peer, "Date", "Time", State)
VALUES ('iwannaeat', '2023-06-27', '10:02', 1),
       ('iwannaeat', '2023-06-27', '13:20', 2),
       ('iwannaeat', '2023-06-27', '13:22', 1),
       ('iwannaeat', '2023-06-27', '19:20', 2),
       ('aboba', '2023-06-2', '10:02', 1),
       ('aboba', '2023-06-27', '19:20', 2),
       ('lalala', '2023-06-28', '12:23', 1),
       ('lalala', '2023-06-28', '21:23', 2),
       ('bus', '2023-07-27', '10:02', 1),
       ('bus', '2023-07-27', '11:20', 2),
       ('bus', '2023-07-27', '11:22', 1),
       ('bus', '2023-07-27', '19:20', 2),
       ('bus', '2023-07-28', '11:22', 1),
       ('bus', '2023-07-28', '19:20', 2);


------------- IMPORT&EXPORT ------------

CREATE OR REPLACE PROCEDURE export_table_to_csv(table_name VARCHAR(255), file_name VARCHAR(255), delimeter CHAR)
AS
$$
BEGIN
    EXECUTE 'COPY ' || table_name || ' TO ' || QUOTE_LITERAL(file_name) || ' WITH DELIMITER ' ||
            QUOTE_LITERAL(delimeter) || ' CSV HEADER';
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE import_csv_to_table(table_name VARCHAR(255), file_name VARCHAR(255), delimeter CHAR)
AS
$$
BEGIN
    EXECUTE 'COPY ' || table_name || ' FROM ' || QUOTE_LITERAL(file_name) || ' WITH DELIMITER ' ||
            QUOTE_LITERAL(delimeter) || ' CSV HEADER';
END;
$$ LANGUAGE PLPGSQL;

-- TEST SCHEMA --
CREATE SCHEMA IF NOT EXISTS test
    AUTHORIZATION pg_database_owner;

COMMENT ON SCHEMA test
    IS 'standard test schema';

GRANT USAGE ON SCHEMA test TO PUBLIC;

GRANT ALL ON SCHEMA test TO pg_database_owner;
