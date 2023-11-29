DROP FUNCTION IF EXISTS TransferredPointsHuman;
DROP FUNCTION IF EXISTS TransferredPointsChange1;
DROP FUNCTION IF EXISTS TransferredPointsChange2;
DROP FUNCTION IF EXISTS MostFrequentlyCheckedTasksByDay;
DROP FUNCTION IF EXISTS PeersCompletedAnyTaskBlock;
DROP FUNCTION IF EXISTS ChecksOnBirthdayStat;
DROP FUNCTION IF EXISTS PeersWithTaskConditionYYN;
DROP FUNCTION IF EXISTS GetLuckyDays;
DROP FUNCTION IF EXISTS MonthsEarlyEntries;
DROP FUNCTION IF EXISTS GetSuccessfulReview;
DROP FUNCTION IF EXISTS GetCampusPeers;
DROP FUNCTION IF EXISTS ReviewAndFriends;
DROP FUNCTION IF EXISTS PeersAndBlocksOfTasks;
DROP FUNCTION IF EXISTS PeersViewDaysNMoreM;
DROP FUNCTION IF EXISTS PeersLeftCampusInNDaysNMoreM;
DROP FUNCTION IF EXISTS PeerWithMaxXP;
DROP FUNCTION IF EXISTS CountParent;

----------------- 3_1 -----------------
-- таблицу TransferredPoints в более --
-------  человекочитаемом виде --------
------- Ник пира 1, ник пира 2, -------
-- количество переданных пир поинтов --
---------------------------------------
CREATE
    OR REPLACE FUNCTION TransferredPointsHuman()
    RETURNS TABLE
            (
                Peer1        VARCHAR(40),
                Peer2        VARCHAR(40),
                PointsAmount INT
            )
AS
$$
BEGIN
    RETURN QUERY WITH TransferredPointsPairs AS
                          (SELECT p1.Id           AS p1_Id
                                , p1.CheckingPeer AS p1_CheckingPeer
                                , p1.CheckedPeer  AS p1_CheckedPeer
                                , p1.PointsAmount AS p1_PointsAmount
                                , p2.Id           AS p2_Id
                                , p2.CheckingPeer AS p2_CheckingPeer
                                , p2.CheckedPeer  AS p2_CheckedPeer
                                , p2.PointsAmount AS p2_PointsAmount
                           FROM TransferredPoints p1
                                    JOIN TransferredPoints p2
                                         ON p1.CheckingPeer = p2.CheckedPeer AND p1.CheckedPeer = p2.CheckingPeer AND
                                            p1.Id >= p2.Id)

                 SELECT CheckingPeer AS Peer1
                      , CheckedPeer  AS Peer2
                      , TransferredPoints.PointsAmount
                 FROM TransferredPoints
                 WHERE NOT EXISTS(SELECT 1 FROM TransferredPointsPairs WHERE Id = p1_id OR Id = p2_id)

                 UNION

                 SELECT p1_CheckingPeer                   AS Peer1
                      , p1_CheckedPeer                    AS Peer2
                      , p1_PointsAmount - p2_PointsAmount AS PointsAmount
                 FROM TransferredPointsPairs

                 ORDER BY Peer1, Peer2;

END
$$
    LANGUAGE plpgsql;

---------------- 3_2 ----------------
--- Таблица вида: ник пользователя --
--- название проверенного задания ---
------- кол-во полученного XP -------
-------------------------------------
CREATE OR REPLACE FUNCTION GetSuccessfulReview()
    RETURNS TABLE
            (
                peer VARCHAR(40),
                task VARCHAR,
                xp   INT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT c.peer     AS peer,
               c.task     AS task,
               x.xpamount AS xp
        FROM Checks c
                 JOIN verter v ON c.id = v."Check"
                 JOIN xp x ON c.id = x."Check"
        WHERE v.state = 'Success';
END;
$$ LANGUAGE PLPGSQL;

--------------- 3_3 ---------------
--- пиры, которые не выходили из --
--- кампуса в течение всего дня ---
-----------------------------------
CREATE OR REPLACE FUNCTION GetCampusPeers(_date DATE)
    RETURNS TABLE
            (
                peer VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT t.peer
        FROM timetracking t
        WHERE "Date" = _date
          AND peer NOT IN (SELECT t.peer
                           FROM timetracking t
                           WHERE "Date" = _date
                             AND state = 2)
        GROUP BY t.peer;
END;
$$ LANGUAGE PLPGSQL;

------------------ 3_4 ------------------
--- Посчитать изменение в количестве ----
-- пир поинтов каждого пира по таблице --
----------- TransferredPoints -----------
-----------------------------------------
CREATE
    OR REPLACE FUNCTION TransferredPointsChange1()
    RETURNS TABLE
            (
                Peer         VARCHAR(40),
                PointsChange BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT CASE WHEN a.Peer IS NULL THEN b.Peer ELSE a.Peer END,
               CASE
                   WHEN b.Negative IS NULL THEN a.Positive
                   WHEN a.Positive IS NULL THEN 0 - b.Negative
                   ELSE a.Positive - b.Negative END AS PointsChange
        FROM (SELECT CheckingPeer AS Peer, SUM(PointsAMount) AS Positive FROM TransferredPoints GROUP BY CheckingPeer) a
                 FULL OUTER JOIN
             (SELECT CheckedPeer AS Peer, SUM(PointsAmount) AS Negative FROM TransferredPoints GROUP BY CheckedPeer) b
             ON a.Peer = b.Peer
        ORDER BY PointsChange DESC, Peer;
END
$$
    LANGUAGE plpgsql;

------------------- 3_5 --------------------
----- Посчитать изменение в количестве -----
--- пир поинтов каждого пира по таблице, ---
-- возвращаемой первой функцией из Part 3 --
--------------------------------------------
CREATE
    OR REPLACE FUNCTION TransferredPointsChange2()
    RETURNS TABLE
            (
                Peer         VARCHAR(40),
                PointsChange BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT CASE WHEN a.Peer IS NULL THEN b.Peer ELSE a.Peer END,
               CASE
                   WHEN b.Negative IS NULL THEN a.Positive
                   WHEN a.Positive IS NULL THEN 0 - b.Negative
                   ELSE a.Positive - b.Negative END AS PointsChange
        FROM (SELECT Peer1 AS Peer, SUM(PointsAMount) AS Positive FROM TransferredPointsHuman() GROUP BY Peer1) a
                 FULL OUTER JOIN
             (SELECT Peer2 AS Peer, SUM(PointsAmount) AS Negative FROM TransferredPointsHuman() GROUP BY Peer2) b
             ON a.Peer = b.Peer
        ORDER BY PointsChange DESC, Peer;
END
$$
    LANGUAGE plpgsql;

------------- 3_6 -------------
--- Самое часто проверяемое ---
--- задание за каждый день ----
-------------------------------
CREATE
    OR REPLACE FUNCTION MostFrequentlyCheckedTasksByDay()
    RETURNS TABLE
            (
                DAY  DATE,
                Task VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH TasksFrequencyByDay AS
                 (SELECT Checks."Date" AS DAY, Checks.Task, COUNT(Checks.Task) AS count_
                  FROM Checks
                  GROUP BY Checks."Date", Checks.Task),
             ChecksFrequencyByDay AS
                 (SELECT tf.Day, MAX(count_) AS max_
                  FROM TasksFrequencyByDay tf
                  GROUP BY tf.Day)
        SELECT tf.Day, tf.Task
        FROM TasksFrequencyByDay tf
                 JOIN ChecksFrequencyByDay cf ON tf.Day = cf.Day
        WHERE count_ = max_;
END
$$
    LANGUAGE plpgsql;

----------------- 3_7 ------------------
---- Найти всех пиров, выполнивших -----
------ весь заданный блок задач и ------
-- дату завершения последнего задания --
----------------------------------------
CREATE
    OR REPLACE FUNCTION PeersCompletedAnyTaskBlock()
    RETURNS TABLE
            (
                Peer VARCHAR(40),
                Task VARCHAR(40),
                DAY  DATE
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT c.Peer, c.Task, MAX(c."Date") AS DAY
        FROM Checks C
                 JOIN P2P p
                      ON C.Id = p."Check"
        WHERE p.State = 'Success'
          AND C.Task IN (
                         'DO2_Linux_Network', 'C8_3DViewer_v1.0', 'CPP1_s21_matrinx+')
        GROUP BY C.Peer, C.Task, C."Date";
END
$$
    LANGUAGE plpgsql;

------------- 3_8 -------------
-- Определить, к какому пиру --
--- стоит идти на проверку ----
---- каждому обучающемуся -----
-------------------------------
CREATE OR REPLACE FUNCTION ReviewAndFriends()
    RETURNS TABLE
            (
                Peer           VARCHAR(40),
                RecomendedPeer VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH all_friends AS ((SELECT peer1, peer2 FROM friends)
                             UNION
                             (SELECT peer2 AS peer1, peer1 AS peer2 FROM friends)),
             friens_recomendations AS (SELECT peer1, peer2 AS friend, recommendedpeer
                                       FROM all_friends af
                                                JOIN recommendations r ON af.peer2 = r.peer),
             count_all AS (SELECT peer1, recommendedpeer, COUNT(*) AS count_
                           FROM friens_recomendations fr
                           WHERE peer1 <> recommendedpeer
                           GROUP BY 1, 2),
             max_c AS (SELECT peer1, recommendedpeer, MAX(count_) OVER (PARTITION BY peer1) AS mc FROM count_all)
        SELECT ca.peer1, ca.recommendedpeer
        FROM count_all ca
        WHERE (peer1, count_) IN (SELECT m.peer1, m.mc FROM max_c m);
END;
$$ LANGUAGE PLPGSQL;

------------------- 3_9 ------------------
--- Определить процент пиров, которые: ---
-----– Приступили только к блоку 1 -------
------ Приступили только к блоку 2 -------
----------- Приступили к обоим -----------
------- Не приступили ни к одному --------
------------------------------------------
CREATE OR REPLACE FUNCTION PeersAndBlocksOfTasks()
    RETURNS TABLE
            (
                StartedBlock1      NUMERIC,
                StartedBlock2      NUMERIC,
                StartedBothBlocks  NUMERIC,
                DidntStartAnyBlock NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH Block1 AS (SELECT peer FROM checks WHERE task ~ '^C[1-9]*' GROUP BY peer),
             Block2 AS (SELECT peer FROM checks WHERE task ~ '^D0*' GROUP BY peer),
             BothBlocks AS (SELECT peer FROM Block1 WHERE peer IN (SELECT peer FROM Block2)),
             NoBlock AS (SELECT nickname AS peer
                         FROM peers
                         WHERE nickname NOT IN
                               ((SELECT peer FROM Block1) UNION (SELECT peer FROM Block2))
                         GROUP BY peer),
             CountBlock1 AS (SELECT COUNT(*) AS count_ FROM Block1),
             CountBlock2 AS (SELECT COUNT(*) AS count_ FROM Block2),
             CountNoBlock AS (SELECT COUNT(*) AS count_ FROM NoBlock),
             CountBothBlocks AS (SELECT COUNT(*) AS count_ FROM BothBlocks),
             CountAllPeer AS (SELECT COUNT(*) AS count_ FROM peers)
        SELECT ROUND((b1.count_ / cap.count_ ::NUMERIC * 100), 2) AS StartedBlock1,
               ROUND((b2.count_ / cap.count_ ::NUMERIC * 100), 2) AS StartedBlock2,
               ROUND((bb.count_ / cap.count_ ::NUMERIC * 100), 2) AS StartedBothBlocks,
               ROUND((nb.count_ / cap.count_ ::NUMERIC * 100), 2) AS DidntStartAnyBlock
        FROM CountAllPeer cap,
             CountBlock1 b1,
             CountBlock2 b2,
             CountBothBlocks bb,
             CountNoBlock nb;
END;
$$ LANGUAGE PLPGSQL;

----------------- 3_10 ------------------
---- Найти всех пиров, выполнивших -----
------ весь заданный блок задач и ------
-- дату завершения последнего задания --
----------------------------------------
CREATE
    OR REPLACE FUNCTION ChecksOnBirthdayStat()
    RETURNS TABLE
            (
                SuccessfulChecks   INT,
                UnsuccessfulChecks INT
            )
AS
$$
DECLARE
    success INT;
    failure
            INT;
    entire
            INT;
BEGIN
    entire
        = (SELECT COUNT(*) AS PeersCount FROM Peers);
    failure
        = (SELECT COUNT(*) AS BirthdayFailuresCount
           FROM Peers
           WHERE EXISTS(SELECT 1
                        FROM P2P pp
                                 JOIN Checks C ON pp."Check" = C.Id
                        WHERE pp.State = 'Failure'
                          AND DATE_PART('month', C."Date") = DATE_PART('month', Birthday)
                          AND DATE_PART('day', C."Date") = DATE_PART('day', Birthday)));
    success
        = (SELECT COUNT(*) AS BirthdaySuccessesCount
           FROM Peers
           WHERE EXISTS(SELECT 1
                        FROM P2P pp
                                 JOIN Checks C ON pp."Check" = C.Id
                        WHERE pp.State = 'Success'
                          AND DATE_PART('month', C."Date") = DATE_PART('month', Birthday)
                          AND DATE_PART('day', C."Date") = DATE_PART('day', Birthday)));
    RETURN QUERY
        SELECT (success * 100 / entire)::INT AS SuccessfulChecks, (failure * 100 / entire) ::INT AS UnsuccessfulChecks;
END
$$
    LANGUAGE plpgsql;

---------------- 3_11 ----------------
--- Определить всех пиров, которые ---
--- сдали заданные задания 1 и 2, ----
------- но не сдали задание 3 --------
--------------------------------------
CREATE
    OR REPLACE FUNCTION PeersWithTaskConditionYYN(Task1 VARCHAR(40), Task2 VARCHAR(40), Task3 VARCHAR(40))
    RETURNS TABLE
            (
                Peer VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT Nickname AS Peer
        FROM Peers
        WHERE EXISTS(
                SELECT 1
                FROM checks c
                         JOIN p2p ON c.Id = p2p."Check"
                WHERE c.Peer = Nickname
                  AND c.Task = Task1
                  AND p2p.State = 'Success'
            )
          AND EXISTS(
                SELECT 1
                FROM checks c
                         JOIN p2p ON c.Id = p2p."Check"
                WHERE c.Peer = Nickname
                  AND c.Task = Task2
                  AND p2p.State = 'Success'
            )
          AND NOT EXISTS(
                SELECT 1
                FROM checks c
                         JOIN p2p ON c.Id = p2p."Check"
                WHERE c.Peer = Nickname
                  AND c.Task = Task3
                  AND p2p.State = 'Success'
            );
END
$$
    LANGUAGE plpgsql;

---------------- 3_12 ----------------
--- сколько задач нужно выполнить, ---
------ исходя из условий входа, ------
-- чтобы получить доступ к текущей. --
--------------------------------------
CREATE OR REPLACE FUNCTION CountParent()
    RETURNS TABLE
            (
                Task      VARCHAR,
                PrevCount INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH RECURSIVE r AS (SELECT t1.title,
                                    t1.parenttask,
                                    CASE
                                        WHEN (t1.parenttask IS NULL) THEN 0
                                        ELSE 1
                                        END AS count_
                             FROM tasks t1
                             UNION
                             SELECT t2.title,
                                    r.title AS parenttask,
                                    CASE
                                        WHEN t2.parenttask IS NOT NULL THEN count_ + 1
                                        ELSE count_
                                        END AS count_
                             FROM tasks t2
                                      JOIN r ON r.title LIKE t2.parenttask)
        SELECT title       AS Task,
               MAX(count_) AS PrevCount
        FROM r
        GROUP BY title
        ORDER BY 1;
END;
$$ LANGUAGE PLPGSQL;

-------------------- 3_13 ---------------------
------ Найти "удачные" для проверок дни. ------
-- День считается "удачным", если в нем есть --
-- хотя бы N идущих подряд успешных проверки --
-----------------------------------------------
CREATE
    OR REPLACE FUNCTION GetLuckyDays(LuckyTerm INT)
    RETURNS TABLE
            (
                DAY DATE
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH P2PStateNum AS
                 (SELECT checks."Date"
                       , p2p."Time"
                       , CASE WHEN p2p.State = 'Success' THEN 1 ELSE 0 END AS StateNum
                  FROM p2p
                           JOIN checks ON p2p."Check" = Checks.Id
                  WHERE p2p.State != 'Start'
                  ORDER BY checks."Date", p2p."Time"),
             P2PStateGN AS
                 (SELECT *,
                         (ROW_NUMBER() OVER (ORDER BY "Date", "Time")) -
                         (ROW_NUMBER() OVER (PARTITION BY "Date", statenum ORDER BY "Date", "Time")) AS grn
                  FROM P2PStateNum),
             P2PDaySuccessSeqLength AS
                 (SELECT "Date" AS DATE, COUNT(grn) AS count_ FROM P2PStateGN WHERE stateNum = 1 GROUP BY "Date", grn),
             P2PDayMaxSuccessSeq AS
                 (SELECT DATE, MAX(count_) AS MaxSeq FROM P2PDaySuccessSeqLength GROUP BY DATE)
        SELECT DATE AS DAY
        FROM P2PDayMaxSuccessSeq
        WHERE MaxSeq >= LuckyTerm;
END
$$
    LANGUAGE plpgsql;

-------------- 3_14 --------------
-- Определить пира с наибольшим --
--------- количеством XP ---------
----------------------------------
CREATE OR REPLACE FUNCTION PeerWithMaxXP()
    RETURNS TABLE
            (
                Peer VARCHAR(40),
                XP   BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT nickname      AS Peer,
               SUM(xpamount) AS XP
        FROM peers
                 JOIN checks c ON peers.nickname = c.peer
                 JOIN xp x ON c.id = x."Check"
        GROUP BY nickname
        ORDER BY 2 DESC
        LIMIT 1;
END;
$$ LANGUAGE PLPGSQL;

------------- 3_15 -------------
--- пиры, приходивших раньше ---
-- заданного времени не менее --
------ N раз за всё время ------
--------------------------------
CREATE OR REPLACE FUNCTION PeersViewDaysNMoreM(_time TIME, _count INTEGER)
    RETURNS TABLE
            (
                nickname VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer
        FROM timetracking t
        WHERE t."Time" < _time
          AND state = 1
        GROUP BY t.peer
        HAVING COUNT(*) >= _count;
END;
$$ LANGUAGE PLPGSQL;

-------------- 3_16 --------------
-- Определить пиров, выходивших --
----- за последние N дней из -----
------ кампуса больше M раз ------
CREATE OR REPLACE FUNCTION PeersLeftCampusInNDaysNMoreM(_days INTEGER, _count INTEGER)
    RETURNS TABLE
            (
                nickname VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer
        FROM timetracking t
        WHERE t."Date" > CURRENT_DATE - _days
          AND state = 2
        GROUP BY t.peer
        HAVING COUNT(*) > _count;
END ;
$$ LANGUAGE PLPGSQL;

-------------- 3_17 ---------------
-- Определить для каждого месяца --
------ процент ранних входов ------
-----------------------------------
CREATE
    OR REPLACE FUNCTION MonthsEarlyEntries()
    RETURNS TABLE
            (
                MONTH        TEXT,
                EarlyEntries INT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH TimeTrackingNotHuman AS
                 (SELECT MIN(tt."Time")             AS "Time",
                         DATE_PART('month', "Date") AS MONTH
                  FROM TimeTracking tt
                           JOIN Peers p ON tt.Peer = p.Nickname
                  WHERE DATE_PART('month', "Date") = DATE_PART('month', Birthday)
                    AND tt.State = 1
                  GROUP BY tt.Peer, tt."Date", p.Birthday, tt.State)

        SELECT TO_CHAR(TO_DATE(Early.month::TEXT, 'MM'), 'Month') AS MONTH,
               100 * Early.count / Total.count                    AS EarlyEntries
        FROM (SELECT nh.month, COUNT(*):: INT
              FROM TimeTrackingNotHuman nh
              WHERE "Time" > '12:00:00+07:00'
              GROUP BY nh.month) Early
                 JOIN
             (SELECT nh.month, COUNT(*):: INT
              FROM TimeTrackingNotHuman nh
              GROUP BY nh.month) Total
             ON Early.month = Total.month;
END
$$
    LANGUAGE plpgsql;

-- TESTS & ASSERTION --
-- Random data generation --
DROP FUNCTION IF EXISTS test.GetRandomPeer;
DROP FUNCTION IF EXISTS test.GetRandomTask;
DROP FUNCTION IF EXISTS test.FillXPRandom;

CREATE
    OR REPLACE FUNCTION test.GetRandomPeer()
    RETURNS VARCHAR(40) AS
$$
DECLARE
BEGIN
    RETURN
        (SELECT Nickname
         FROM (SELECT ROW_NUMBER() OVER () AS id, Nickname FROM Peers) a
         WHERE id = ((SELECT COUNT(*) - 1 FROM Peers) * (SELECT RANDOM()) + 1)::INT);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE FUNCTION test.GetRandomTask()
    RETURNS VARCHAR(40) AS
$$
DECLARE
BEGIN
    RETURN
        (SELECT Title
         FROM (SELECT ROW_NUMBER() OVER () AS id, Title FROM Tasks) a
         WHERE id = ((SELECT COUNT(*) - 1 FROM Tasks) * (SELECT RANDOM()) + 1)::INT);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.FillXPRandom(minXPPercent INT, maxXPPercent INT)
AS
$$
BEGIN
    INSERT INTO xp("Check", XPAmount)
    SELECT p2p."Check", tasks.MaxXP * (minXPPercent + (maxXPPercent - minXPPercent) * RANDOM()) / 100
    FROM checks
             JOIN p2p ON p2p.State = 'Success' AND p2p."Check" = checks.Id
             JOIN tasks ON tasks.Title = checks.Task

    WHERE NOT EXISTS(
            SELECT 1 FROM xp WHERE "Check" = checks.Id
        );
END
$$
    LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS test.TestPeers;
DROP PROCEDURE IF EXISTS test.InitTestData;
DROP PROCEDURE IF EXISTS test.CleanTestData;
DROP PROCEDURE IF EXISTS test.Test__TransferredPointsHuman;
DROP PROCEDURE IF EXISTS test.Test__TransferredPointsChange1;
DROP PROCEDURE IF EXISTS test.Test__TransferredPointsChange2;
DROP PROCEDURE IF EXISTS test.Test__MostFrequentlyCheckedTasksByDay;
DROP PROCEDURE IF EXISTS test.Test__PeersCompletedAnyTaskBlock;
DROP PROCEDURE IF EXISTS test.Test__ChecksOnBirthdayStat;
DROP PROCEDURE IF EXISTS test.Test__PeersWithTaskConditionYYN;
DROP PROCEDURE IF EXISTS test.Test__GetLuckyDays;
DROP PROCEDURE IF EXISTS test.Test__MonthsEarlyEntries;
DROP PROCEDURE IF EXISTS test.Test__GetSuccessfulReview;
DROP PROCEDURE IF EXISTS test.Test__PeersViewDaysNMoreM;
DROP PROCEDURE IF EXISTS test.Test__PeersLeftCampusInNDaysNMoreM;
DROP PROCEDURE IF EXISTS test.Test__PeerWithMaxXP;
DROP PROCEDURE IF EXISTS test.Test__CountParent;

CREATE
    OR REPLACE FUNCTION test.TestPeers()
    RETURNS TABLE
            (
                Peer VARCHAR(40)
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT Nickname
        FROM Peers
        WHERE Nickname IN ('Human1', 'Human2', 'Human3', 'Human4', 'Human5', 'Human6');
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.InitTestData()
AS
$$
BEGIN
    -- TransferredPointsHuman
    INSERT INTO Peers(Nickname, Birthday)
    VALUES ('Human1', '2023-07-11'),
           ('Human2', '2000-01-12'),
           ('Human3', '1992-11-29'),
           ('Human4', CURRENT_DATE),
           ('Human5', '2002-07-01'),
           ('Human6', '2000-06-09');

    -- Peer1
    CALL AddP2P('Human1', 'Human2', 'C6_s21_matrix', 'Start', '08:30:00+07:00');
    CALL AddP2P('Human1', 'Human2', 'C6_s21_matrix', 'Success', '08:45:00+07:00');
    CALL AddP2P('Human1', 'Human3', 'C6_s21_matrix', 'Start', '09:30:00+07:00');
    CALL AddP2P('Human1', 'Human3', 'C6_s21_matrix', 'Success', '09:45:00+07:00');
    CALL AddP2P('Human1', 'Human4', 'C6_s21_matrix', 'Start', '10:00:00+07:00');
    CALL AddP2P('Human1', 'Human4', 'C6_s21_matrix', 'Failure', '10:15:00+07:00');
    CALL AddP2P('Human1', 'Human4', 'C6_s21_matrix', 'Start', '10:30:00+07:00');
    CALL AddP2P('Human1', 'Human4', 'C6_s21_matrix', 'Failure', '10:45:00+07:00');

-- Peer 2
    CALL AddP2P('Human4', 'Human1', 'C6_s21_matrix', 'Start', '10:45:00+07:00');
    CALL AddP2P('Human4', 'Human1', 'C6_s21_matrix', 'Failure', '11:00:00+07:00');
    CALL AddP2P('Human2', 'Human1', 'C6_s21_matrix', 'Start', '11:30:00+07:00');
    CALL AddP2P('Human2', 'Human1', 'C6_s21_matrix', 'Success', '11:45:00+07:00');
    CALL AddP2P('Human2', 'Human1', 'C7_SmartCalc_v1.0', 'Start', '11:45:00+07:00');
    CALL AddP2P('Human2', 'Human1', 'C7_SmartCalc_v1.0', 'Success', '12:00:00+07:00');
    CALL AddP2P('Human2', 'Human3', 'DO1_Linux', 'Start', '12:30:00+07:00');
    CALL AddP2P('Human2', 'Human3', 'DO1_Linux', 'Success', '12:45:00+07:00');

-- Peer 3
    CALL AddP2P('Human3', 'Human4', 'DO1_Linux', 'Start', '13:30:00+07:00');
    CALL AddP2P('Human3', 'Human4', 'DO1_Linux', 'Failure', '13:45:00+07:00');
    CALL AddP2P('Human3', 'Human1', 'C8_3DViewer_v1.0', 'Start', '14:30:00+07:00');
    CALL AddP2P('Human3', 'Human1', 'C8_3DViewer_v1.0', 'Success', '14:45:00+07:00');

-- Peer 4
    CALL AddP2P('Human4', 'Human1', 'CPP1_s21_matrix+', 'Start', '14:45:00+07:00');
    CALL AddP2P('Human4', 'Human1', 'CPP1_s21_matrix+', 'Failure', '15:00:00+07:00');

-- Peer 5
    CALL AddP2P('Human5', 'Human6', 'C6_s21_matrix', 'Start', '15:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C6_s21_matrix', 'Success', '15:15:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C7_SmartCalc_v1.0', 'Start', '15:45:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C7_SmartCalc_v1.0', 'Success', '16:00:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C8_3DViewer_v1.0', 'Start', '15:45:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C8_3DViewer_v1.0', 'Failure', '16:00:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C8_3DViewer_v1.0', 'Start', '15:45:00+07:00');
    CALL AddP2P('Human5', 'Human6', 'C8_3DViewer_v1.0', 'Success', '16:00:00+07:00');

-- Peer 6
    CALL AddP2P('Human6', 'Human5', 'DO1_Linux', 'Start', '16:30:00+07:00');
    CALL AddP2P('Human6', 'Human5', 'DO1_Linux', 'Success', '16:45:00+07:00');
    CALL AddP2P('Human6', 'Human5', 'DO2_Linux_Network', 'Start', '16:45:00+07:00');
    CALL AddP2P('Human6', 'Human5', 'DO2_Linux_Network', 'Success', '17:0:00+07:00');

    CALL test.FillXPRandom(80, 100);

    INSERT INTO TimeTracking(Peer, "Date", "Time", State)
    VALUES ('Human2', '2023-01-01', '11:00:00+07:00', 1),
           ('Human2', '2023-01-01', '13:00:00+07:00', 2),
           ('Human2', '2023-01-02', '12:01:00+07:00', 1),
           ('Human2', '2023-01-02', '13:00:00+07:00', 2),
           ('Human6', '2023-06-02', '12:01:00+07:00', 1),
           ('Human6', '2023-06-02', '19:01:00+07:00', 2),
           ('Human6', '2023-06-03', '11:51:00+07:00', 1),
           ('Human6', '2023-06-03', '19:01:00+07:00', 2),
           ('Human6', '2023-06-03', '20:51:00+07:00', 1),
           ('Human6', '2023-06-03', '21:01:00+07:00', 2),
           ('Human1', '2023-07-02', '08:00:00+07:00', 1),
           ('Human1', '2023-07-02', '12:00:00+07:00', 2),
           ('Human1', '2023-07-03', '08:00:00+07:00', 1),
           ('Human1', '2023-07-03', '12:00:00+07:00', 2),
           ('Human1', '2023-07-04', '13:00:00+07:00', 1),
           ('Human1', '2023-07-04', '14:00:00+07:00', 2),
           ('Human1', '2023-07-05', '09:00:00+07:00', 1),
           ('Human1', '2023-07-05', '12:00:00+07:00', 2),
           ('Human4', '2023-07-05', '09:00:00+07:00', 1),
           ('Human4', '2023-07-05', '12:00:00+07:00', 2),
           ('Human4', '2023-07-06', '09:00:00+07:00', 1),
           ('Human4', '2023-07-06', '12:00:00+07:00', 2);

END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.CleanTestData()
AS
$$
BEGIN
    DELETE
    FROM P2P
    WHERE CheckingPeer IN (SELECT * FROM test.TestPeers());
    DELETE
    FROM XP
    WHERE "Check" IN (SELECT Id FROM Checks WHERE Peer IN (SELECT * FROM test.TestPeers()));
    DELETE
    FROM Checks
    WHERE Peer IN (SELECT * FROM test.TestPeers());
    DELETE
    FROM TransferredPoints
    WHERE CheckingPeer IN (SELECT * FROM test.TestPeers());
    DELETE
    FROM TimeTracking
    WHERE Peer IN (SELECT * FROM test.TestPeers());
    DELETE
    FROM Peers
    WHERE Nickname IN (SELECT * FROM test.TestPeers());
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__TransferredPointsHuman()
AS
$$
BEGIN
    ASSERT 1 = (SELECT PointsAmount FROM TransferredPointsHuman() WHERE Peer1 = 'Human2' AND Peer2 = 'Human1');
    ASSERT
            1 = (SELECT PointsAmount FROM TransferredPointsHuman() WHERE Peer1 = 'Human2' AND Peer2 = 'Human3');
    ASSERT
            0 = (SELECT PointsAmount FROM TransferredPointsHuman() WHERE Peer1 = 'Human3' AND Peer2 = 'Human1');
    ASSERT
            1 = (SELECT PointsAmount FROM TransferredPointsHuman() WHERE Peer1 = 'Human3' AND Peer2 = 'Human4');
    ASSERT
            0 = (SELECT PointsAmount FROM TransferredPointsHuman() WHERE Peer1 = 'Human4' AND Peer2 = 'Human1');
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__TransferredPointsChange1()
AS
$$
BEGIN
    ASSERT 2 = (SELECT PointsChange FROM TransferredPointsChange1() WHERE peer = 'Human2');
    ASSERT
            0 = (SELECT PointsChange FROM TransferredPointsChange1() WHERE peer = 'Human3');
    ASSERT
            -1 = (SELECT PointsChange FROM TransferredPointsChange1() WHERE peer = 'Human1');
    ASSERT
            -1 = (SELECT PointsChange FROM TransferredPointsChange1() WHERE peer = 'Human4');
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__TransferredPointsChange2()
AS
$$
BEGIN
    ASSERT 2 = (SELECT PointsChange FROM TransferredPointsChange2() WHERE peer = 'Human2');
    ASSERT
            0 = (SELECT PointsChange FROM TransferredPointsChange2() WHERE peer = 'Human3');
    ASSERT
            -1 = (SELECT PointsChange FROM TransferredPointsChange2() WHERE peer = 'Human1');
    ASSERT
            -1 = (SELECT PointsChange FROM TransferredPointsChange2() WHERE peer = 'Human4');
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__MostFrequentlyCheckedTasksByDay()
AS
$$
BEGIN
    ASSERT 'C6_s21_matrix' = (SELECT Task FROM MostFrequentlyCheckedTasksByDay() WHERE DAY = CURRENT_DATE);
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__PeersCompletedAnyTaskBlock()
AS
$$
BEGIN
    ASSERT 'Human1' IN (SELECT Peer FROM PeersCompletedAnyTaskBlock());
    ASSERT
        'Human5' IN (SELECT Peer FROM PeersCompletedAnyTaskBlock());
    ASSERT
        'Human6' IN (SELECT Peer FROM PeersCompletedAnyTaskBlock());
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__ChecksOnBirthdayStat()
AS
$$
BEGIN
    ASSERT 7 = (SELECT SuccessfulChecks FROM ChecksOnBirthdayStat());
    ASSERT
        7 = (SELECT UnsuccessfulChecks FROM ChecksOnBirthdayStat());
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__PeersWithTaskConditionYYN()
AS
$$
BEGIN
    ASSERT 'Human1' IN (SELECT *
                        FROM PeersWithTaskConditionYYN('C6_s21_matrix',
                                                       'C7_SmartCalc_v1.0',
                                                       'DO1_Linux')
                        WHERE Peer IN (SELECT * FROM test.TestPeers()));
    ASSERT
            'Human1' IN (SELECT *
                         FROM PeersWithTaskConditionYYN('C7_SmartCalc_v1.0',
                                                        'C8_3DViewer_v1.0',
                                                        'CPP1_s21_matrix+')
                         WHERE Peer IN (SELECT * FROM test.TestPeers()));
    ASSERT
            'Human6' IN (SELECT *
                         FROM PeersWithTaskConditionYYN('C6_s21_matrix',
                                                        'C7_SmartCalc_v1.0',
                                                        'DO1_Linux')
                         WHERE Peer IN (SELECT * FROM test.TestPeers()));
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__GetLuckyDays()
AS
$$
BEGIN
    ASSERT CURRENT_DATE IN (SELECT * FROM GetLuckyDays(2));
    ASSERT
        CURRENT_DATE IN (SELECT * FROM GetLuckyDays(3));
    ASSERT
        CURRENT_DATE IN (SELECT * FROM GetLuckyDays(4));
    ASSERT
        CURRENT_DATE NOT IN (SELECT * FROM GetLuckyDays(5));
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__MonthsEarlyEntries()
AS
$$
BEGIN
    ASSERT 50 = (SELECT earlyentries FROM MonthsEarlyEntries() WHERE MONTH LIKE 'January%');
    ASSERT
            50 = (SELECT earlyentries FROM MonthsEarlyEntries() WHERE MONTH LIKE 'June%');
    ASSERT
            25 = (SELECT earlyentries FROM MonthsEarlyEntries() WHERE MONTH LIKE 'July%');
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.Test__GetSuccessfulReview()
AS
$$
BEGIN
    ASSERT 'aboba' = (SELECT peer FROM GetSuccessfulReview() WHERE task LIKE 'C6_s21_matrix');
    ASSERT
            'C6_s21_matrix' = (SELECT task FROM GetSuccessfulReview() WHERE peer LIKE 'aboba');
END
$$
    LANGUAGE plpgsql;



CREATE
    OR REPLACE PROCEDURE test.Test__ReviewAndFriends()
AS
$$
BEGIN
    ASSERT 'aboba' = (SELECT peer FROM ReviewAndFriends() WHERE RecomendedPeer LIKE 'iwannaeat' ORDER BY 1 LIMIT 1);
    ASSERT 'helpme' = (SELECT Peer FROM ReviewAndFriends() WHERE RecomendedPeer LIKE 'aboba' ORDER BY 1 LIMIT 1);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.Test__PeersAndBlocksOfTasks()
AS
$$
BEGIN
    ASSERT 1 > (14 - (SELECT StartedBothBlocks FROM PeersAndBlocksOfTasks()));
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.Test__PeersViewDaysNMoreM()
AS
$$
BEGIN
    ASSERT 'lalala' = (SELECT * FROM PeersViewDaysNMoreM('19:00', 1) ORDER BY 1 DESC LIMIT 1);
    ASSERT 'bus' = (SELECT * FROM PeersViewDaysNMoreM('23:00', 2) ORDER BY 1 LIMIT 1);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.Test__PeersLeftCampusInNDaysNMoreM()
AS
$$
BEGIN
    ASSERT 'bus' = (SELECT nickname FROM PeersLeftCampusInNDaysNMoreM(100, 2) LIMIT 1);
    ASSERT 'iwannaeat' = (SELECT nickname FROM PeersLeftCampusInNDaysNMoreM(100, 1) ORDER BY 1 DESC LIMIT 1);
END
$$
    LANGUAGE plpgsql;

CREATE
    OR REPLACE PROCEDURE test.Test__CountParent()
AS
$$
BEGIN
    ASSERT 'CPP1_s21_matrix+' = (SELECT Task FROM CountParent() WHERE PrevCount = 3);
    ASSERT 'C6_s21_matrix' = (SELECT Task FROM CountParent() WHERE PrevCount = 0);
    ASSERT 2 = (SELECT PrevCount FROM CountParent() WHERE Task = 'DO2_Linux_Network');
END
$$
    LANGUAGE plpgsql;


CREATE
    OR REPLACE PROCEDURE test.Test__Part3()
AS
$$
BEGIN
    CALL test.CleanTestData();
    CALL test.InitTestData();

    CALL test.Test__TransferredPointsHuman();
    CALL test.Test__TransferredPointsChange1();
    CALL test.Test__TransferredPointsChange2();
    CALL test.Test__MostFrequentlyCheckedTasksByDay();
    CALL test.Test__PeersCompletedAnyTaskBlock();
    CALL test.Test__ChecksOnBirthdayStat();
    CALL test.Test__PeersWithTaskConditionYYN();
    CALL test.Test__GetLuckyDays();
    CALL test.Test__MonthsEarlyEntries();
    CALL test.Test__GetSuccessfulReview();
    CALL test.Test__ReviewAndFriends();
    CALL test.Test__PeersAndBlocksOfTasks();
    CALL test.Test__PeersViewDaysNMoreM();
    CALL test.Test__PeersLeftCampusInNDaysNMoreM();
    CALL test.Test__CountParent();

    CALL test.CleanTestData();
END
$$
    LANGUAGE plpgsql;

CALL test.Test__Part3();
