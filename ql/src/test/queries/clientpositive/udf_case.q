DESCRIBE FUNCTION case;
DESCRIBE FUNCTION EXTENDED case;

EXPLAIN
SELECT CASE 1
        WHEN 1 THEN 2
        WHEN 3 THEN 4
        ELSE 5
       END,
       CASE 2
        WHEN 1 THEN 2
        ELSE 5
       END,
       CASE 14
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 16
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 17
        WHEN 18 THEN NULL
        WHEN 17 THEN 20
       END,
       CASE 21
        WHEN 22 THEN 23
        WHEN 21 THEN 24
       END
FROM src LIMIT 1;

SELECT CASE 1
        WHEN 1 THEN 2
        WHEN 3 THEN 4
        ELSE 5
       END,
       CASE 2
        WHEN 1 THEN 2
        ELSE 5
       END,
       CASE 14
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 16
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 17
        WHEN 18 THEN NULL
        WHEN 17 THEN 20
       END,
       CASE 21
        WHEN 22 THEN 23
        WHEN 21 THEN 24
       END
FROM src LIMIT 1;

-- verify that short-circuiting is working correctly for CASE
-- we should never get to the ELSE branch, which would raise an exception
SELECT CASE 1 WHEN 1 THEN 'yo'
ELSE reflect('java.lang.String', 'bogus', 1) END
FROM src LIMIT 1;


-- Allow compatible types to be used in return value
SELECT CASE
        WHEN 1=1 THEN 123L
        ELSE 0S
       END,
       CASE
        WHEN 1=1 THEN 123L
        WHEN 1=2 THEN 1
        ELSE 0Y
       END
FROM src LIMIT 1;
