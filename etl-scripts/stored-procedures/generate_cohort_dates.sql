CREATE DEFINER=`henrykorir`@`%` PROCEDURE `generate_cohort_dates`()
BEGIN
DECLARE month_index INT DEFAULT 1;
DECLARE month_start_date VARCHAR(100) DEFAULT "";
DECLARE max_end_date DATETIME DEFAULT NULL;
DECLARE end_date DATETIME DEFAULT NULL;
DECLARE MAX_MONTHS INT DEFAULT 12;
DECLARE end_day DATETIME DEFAULT CONCAT((YEAR(NOW()) + 1),"-12-31");
DECLARE start_year INT DEFAULT 2000;

CREATE TABLE IF NOT EXISTS dates (
       endDate DATETIME,
       INDEX endDate (endDate)
);

SELECT MAX(endDate) INTO max_end_date FROM dates;
-- SELECT max_end_date;

IF (max_end_date IS NULL) THEN
   SET start_year := 2000;
ELSE
   SET start_year := YEAR(max_end_date) + 1;
END IF;

-- SELECT start_year;
LOOP_PER_YEAR: LOOP
   WHILE (month_index <= MAX_MONTHS) DO 
   
        SET month_start_date := CONCAT(start_year,'-',month_index,'-01');
       
        SELECT LAST_DAY(month_start_date) INTO end_date;
   
        INSERT INTO dates (endDate) VALUES(end_date);
   
        SET month_index := month_index + 1;
  
   END WHILE;
   SET month_index := 1;
   SET start_year := start_year + 1;
   IF (start_year > YEAR(end_day)) THEN
      LEAVE LOOP_PER_YEAR; -- exit years loop
   END IF;
 END LOOP;
END