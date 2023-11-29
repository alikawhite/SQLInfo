DROP FUNCTION IF EXISTS IsP2PIncomplete;
DROP FUNCTION IF EXISTS GetP2PCheckId;
DROP FUNCTION IF EXISTS GetP2PTransferredPoints;
DROP FUNCTION IF EXISTS AddP2P;
DROP FUNCTION IF EXISTS AddVerterCheck;

DROP TRIGGER IF EXISTS trg_after_insert_p2p ON P2P;
DROP FUNCTION IF EXISTS fnc_trg_insert_or_update_TransferredPoints;
DROP TRIGGER IF EXISTS trg_before_insert_verter ON xp;
DROP FUNCTION IF EXISTS fnc_trg_before_insert_verter;

DROP PROCEDURE IF EXISTS test.Test__Part2;
DROP PROCEDURE IF EXISTS test.assert_AddP2P;
DROP PROCEDURE IF EXISTS test.assert_AddP2P_parallel;

-- 1 --
CREATE
    OR REPLACE FUNCTION IsP2PIncomplete(
    _checkingPeer VARCHAR(40), _checkedPeer VARCHAR(40), _task VARCHAR(255))
    RETURNS BOOLEAN
AS
$$
DECLARE
    _count INT;
BEGIN
    _count
        = (SELECT COUNT(*)
           FROM P2P p
                    JOIN Checks C ON p."Check" = C.ID AND C.Task = _task
           WHERE C.Task = _task
             AND C.Peer = _checkedPeer
             AND p.CheckingPeer = _checkingPeer);
    IF
        _count % 2 = 0
    THEN
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE FUNCTION GetP2PCheckId(
    _checkingPeer VARCHAR(40), _checkedPeer VARCHAR(40), _task VARCHAR(255))
    RETURNS INT AS
$$
BEGIN
    RETURN (SELECT C.Id
            FROM P2P p
                     JOIN Checks C ON p."Check" = C.ID
            WHERE C.Task = _task
              AND C.Peer = _checkedPeer
              AND p.CheckingPeer = _checkingPeer
            ORDER BY C.Id DESC
            LIMIT 1);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE FUNCTION GetP2PTransferredPoints(
    _checkingPeer VARCHAR(40), _checkedPeer VARCHAR(40))
    RETURNS INT AS
$$
DECLARE
    _points INT;
BEGIN
    _points
        = (SELECT PointsAmount
           FROM TransferredPoints
           WHERE CheckingPeer = _checkingPeer
             AND CheckedPeer = _checkedPeer);
    IF
        _points IS NULL THEN
        RETURN 0;
    END IF;
    RETURN _points;
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE AddP2P(
    _checkingPeer VARCHAR(40),
    _peer VARCHAR(40),
    _task VARCHAR(255),
    _status check_status,
    _time TIME WITH TIME ZONE)
AS
$$
DECLARE
    check_id INT;
BEGIN
    IF
        _status = 'Start' THEN
        IF IsP2PIncomplete(_checkingPeer, _peer, _task) THEN
            RAISE 'task % already started', _task USING ERRCODE = 'invalid_parameter_value';
        END IF;

        INSERT INTO checks(Peer, Task, "Date")
        VALUES (_peer, _task, CURRENT_DATE)
        RETURNING ID
            INTO check_id;
        INSERT INTO p2p("Check", CheckingPeer, State, "Time")
        VALUES (check_id, _checkingPeer, _status, _time);
    ELSE
        INSERT INTO p2p("Check", CheckingPeer, STATE, "Time")
        VALUES (GetP2PCheckId(_checkingPeer, _peer, _task), _checkingPeer, _status, _time);
    END IF;
END
$$
    LANGUAGE plpgsql;


-- 2 --
CREATE
    OR REPLACE PROCEDURE AddVerterCheck(_nickname VARCHAR(40), _task VARCHAR,
                                        _status check_status, _time TIME)
    LANGUAGE PLPGSQL
AS
$$
DECLARE
    check_id INTEGER := (SELECT checks.id
                         FROM checks
                                  JOIN p2p p ON checks.id = p."Check"
                         WHERE p.state = 'Success'
                           AND checks.task = _task
                           AND checks.peer = _nickname
                         ORDER BY p."Time" DESC
                         LIMIT 1);
BEGIN
    IF check_id != 0 THEN
        INSERT INTO verter(id, "Check", STATE, "Time")
        VALUES ((SELECT MAX(ID) FROM verter) + 1, check_id, _status, _time);
    ELSE
        RAISE EXCEPTION 'Ошибка записи в таблицу Verter %', check_id;
    END IF;
END ;
$$;


-- 3 --
CREATE
    OR REPLACE FUNCTION fnc_trg_insert_or_update_TransferredPoints()
    RETURNS TRIGGER
AS
$P2P$
BEGIN
    IF NOT EXISTS(SELECT 1
                  FROM TransferredPoints
                  WHERE CheckingPeer = NEW.CheckingPeer
                    AND CheckedPeer = (SELECT Peer FROM Checks WHERE Id = NEW."Check"))
    THEN
        INSERT INTO TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount)
        SELECT new.CheckingPeer, c.Peer, 1
        FROM Checks c
        WHERE c.Id = new."Check";
    ELSEIF
        NEW.State = 'Start' THEN
        UPDATE TransferredPoints
        SET PointsAmount = PointsAmount + 1
        WHERE CheckingPeer = new.CheckingPeer
          AND CheckedPeer = (SELECT Peer
                             FROM Checks
                             WHERE Id = new."Check"
                             ORDER BY "Date" DESC
                             LIMIT 1);
    END IF;
    RETURN NEW;
END
$P2P$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE TRIGGER trg_after_insert_p2p
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_insert_or_update_TransferredPoints();


-- 4 --
CREATE
    OR REPLACE FUNCTION fnc_trg_before_insert_verter() RETURNS TRIGGER AS
$XP$
DECLARE
    max_xp INT          := (SELECT maxxp
                            FROM tasks t
                                     JOIN checks C ON t.title = C.task
                            WHERE C.id = NEW."Check");
    status
           check_status := (SELECT STATE
                            FROM verter
                            ORDER BY id DESC
                            LIMIT 1);
BEGIN
    IF
        max_xp < NEW.XPAmount OR status != 'Success' THEN
        RAISE EXCEPTION 'Ошибка записи в таблицу!!!';
    END IF;
    RETURN NEW;
END
$XP$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE TRIGGER trg_before_insert_verter
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_before_insert_verter();


-- TESTS & ASSERTION --

CREATE
    OR REPLACE PROCEDURE test.assert_AddP2P(
    _checkingPeer VARCHAR(40),
    _checkedPeer VARCHAR(40),
    _task VARCHAR(255),
    _status check_status)
AS
$$
DECLARE
    _startCheckId INT;
    _successCheckId
                  INT;
    _pointsBefore
                  INT;
    _pointsAfter
                  INT;
BEGIN
    _pointsBefore
        = GetP2PTransferredPoints(_checkingPeer, _checkedPeer);
    CALL AddP2P(_checkingPeer, _checkedPeer, _task, 'Start', '08:00:00+07:00');
    _startCheckId
        = GetP2PCheckId(_checkingPeer, _checkedPeer, _task);
    _pointsAfter
        = GetP2PTransferredPoints(_checkingPeer, _checkedPeer);

    ASSERT
        _pointsBefore + 1 = _pointsAfter, 'Transfered points: % (before) % (start)', _pointsBefore, _pointsAfter;

    _pointsBefore
        = _pointsAfter;
    CALL AddP2P(_checkingPeer, _checkedPeer, _task, _status, '08:30:00+07:00');
    _successCheckId
        = GetP2PCheckId(_checkingPeer, _checkedPeer, _task);
    _pointsAfter
        = GetP2PTransferredPoints(_checkingPeer, _checkedPeer);

    ASSERT
        _startCheckId = _successCheckId , 'Check ID incorrect';
    ASSERT
        _pointsBefore = _pointsAfter, 'Transfered points count incorrect after p2p %', _status;
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.assert_AddP2P_parallel(
    _checkingPeer1 VARCHAR(40),
    _checkedPeer1 VARCHAR(40),
    _task1 VARCHAR(255),
    _checkingPeer2 VARCHAR(40),
    _checkedPeer2 VARCHAR(40),
    _task2 VARCHAR(255))
AS
$$
DECLARE
    _startCheckId1 INT;
    _finishCheckId1
                   INT;
    _startCheckId2
                   INT;
    _finishCheckId2
                   INT;
BEGIN
    CALL AddP2P(_checkingPeer1, _checkedPeer1, _task1, 'Start', '08:00:00+07:00');
    _startCheckId1
        = GetP2PCheckId(_checkingPeer1, _checkedPeer1, _task1);
    CALL AddP2P(_checkingPeer2, _checkedPeer2, _task2, 'Start', '08:02:00+07:00');
    _startCheckId2
        = GetP2PCheckId(_checkingPeer2, _checkedPeer2, _task2);

    CALL AddP2P(_checkingPeer1, _checkedPeer1, _task1, 'Success', '08:30:00+07:00');
    _finishCheckId1
        = GetP2PCheckId(_checkingPeer1, _checkedPeer1, _task1);
    CALL AddP2P(_checkingPeer2, _checkedPeer2, _task2, 'Success', '08:45:00+07:00');
    _finishCheckId2
        = GetP2PCheckId(_checkingPeer2, _checkedPeer2, _task2);

    ASSERT
        _startCheckId1 = _finishCheckId1, '1';
    ASSERT
        _startCheckId2 = _finishCheckId2, '2';

END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.assert_AddVerterCheck(
    _nickname VARCHAR(40),
    _task VARCHAR(255),
    _status check_status,
    _time TIME)
AS
$$
BEGIN
    CALL
        AddVerterCheck(
                _nickname, _task, _status,
                _time);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.Test__Part2() AS
$$
BEGIN
    CALL test.assert_AddP2P('aboba', 'amogus', 'DO1_Linux', 'Success');
    CALL test.assert_AddP2P('aboba', 'amogus', 'C8_3DViewer_v1.0', 'Failure');

    CALL test.assert_AddP2P('iwannaeat', 'aboba', 'C8_3DViewer_v1.0', 'Success');
    CALL test.assert_AddP2P('iwannaeat', 'amogus', 'CPP1_s21_matrix+', 'Failure');

    CALL test.assert_AddP2P_parallel('iwannaeat', 'aboba', 'C8_3DViewer_v1.0',
                                     'iwannaeat', 'amogus', 'CPP1_s21_matrix+');
    CALL
        test.assert_AddVerterCheck('amogus', 'DO1_Linux', 'Start', '17:20:00');
    CALL
        test.assert_AddVerterCheck('amogus', 'DO1_Linux', 'Success', '17:20:04');
END
$$
    LANGUAGE plpgsql;

CALL test.Test__Part2();



