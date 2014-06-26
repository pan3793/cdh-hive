DROP TABLE IF EXISTS DECIMAL;

CREATE TABLE decimal_10_0 (dec decimal);

LOAD DATA LOCAL INPATH '../../data/files/decimal_10_0.txt' OVERWRITE INTO TABLE decimal_10_0;

SELECT dec FROM decimal_10_0;

DROP TABLE decimal_10_0;