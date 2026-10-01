-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### `date`
-- MAGIC
-- MAGIC This function is a synonym for `CAST(expr AS expr)`. 
-- MAGIC
-- MAGIC #### Arguments
-- MAGIC
-- MAGIC - `expr`: An expression that can be cast to `DATE`.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC A DATE.

-- COMMAND ----------

-- DBTITLE 1,date - Syntax
date(expr)

-- COMMAND ----------

-- DBTITLE 1,date - Example
SELECT date('2021-03-21'); -- 2021-03-21

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `current_date`
-- MAGIC
-- MAGIC Returns the current date at the start of query evaluation.
-- MAGIC
-- MAGIC #### Arguments
-- MAGIC
-- MAGIC This function takes no arguments.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC A DATE.
-- MAGIC The braces are optional

-- COMMAND ----------

-- DBTITLE 1,current_date - Syntax
current_date()

-- COMMAND ----------

-- DBTITLE 1,current_date - Examples
SELECT current_date(); -- 2020-04-25

SELECT current_date; -- 2020-04-25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `current_timestamp`
-- MAGIC
-- MAGIC Returns the current timestamp at the start of query evaluation.
-- MAGIC
-- MAGIC #### 
-- MAGIC
-- MAGIC This function takes no arguments.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC A TIMESTAMP.
-- MAGIC The braces are optional.

-- COMMAND ----------

-- DBTITLE 1,current_timestamp - Syntax
current_timestamp()

-- COMMAND ----------

-- DBTITLE 1,current_timestamp - Examples
SELECT current_timestamp(); -- 2020-04-25 15:49:11.914

SELECT current_timestamp; -- 2020-04-25 15:49:11.914

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `date_add`
-- MAGIC
-- MAGIC Adds `value` and `unit` to a timestamp `expr`. This function is a synonym for `timestampadd` function.
-- MAGIC
-- MAGIC #### Arguments
-- MAGIC
-- MAGIC - `unit`: A `unit` of measure.
-- MAGIC - `value`: A numeric expression with the number of units to add to `expr`.
-- MAGIC - `expr`: A TIMESTAMP expression.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC A TIMESTAMP.
-- MAGIC
-- MAGIC If `value` is negative, it is subtracted from the `expr`. If `unit` is `MONTH`, `QUARTER`, or `YEAR` the day portion of the result will be adjusted to result in a valid date.
-- MAGIC
-- MAGIC The function returns an overflow error if the result is beyond the supported range of timestamps.

-- COMMAND ----------

-- DBTITLE 1,date_add - Syntax
date_add(unit, value, expr)

unit
 { MICROSECOND |
   MILLISECOND |
   SECOND |
   MINUTE |
   HOUR |
   DAY | DAYOFYEAR |
   WEEK |
   MONTH |
   QUARTER |
   YEAR }

-- COMMAND ----------

-- DBTITLE 1,date_add - Examples
SELECT date_add(MICROSECOND, 5, TIMESTAMP'2022-02-28 00:00:00'); -- 2022-02-28 00:00:00.000005

-- March 31. 2022 minus 1 month yields February 28. 2022
SELECT date_add(MONTH, -1, TIMESTAMP'2022-03-31 00:00:00'); -- 2022-02-28 00:00:00.000000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `date_diff`
-- MAGIC
-- MAGIC Returns the difference between two timestamps measured in units. `date_diff` (timestamp) is a synonym for `timestampdiff` function.
-- MAGIC
-- MAGIC #### Arguments
-- MAGIC
-- MAGIC - `unit`: A unit of measure.
-- MAGIC - `start`: A starting TIMESTAMP expression.
-- MAGIC - `end`: A ending TIMESTAMP expression.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC A `BIGINT`.
-- MAGIC
-- MAGIC - If `start` is greater than `end` the result is negative.
-- MAGIC
-- MAGIC - The function counts whole elapsed units based on UTC with a DAY being 86400 seconds.
-- MAGIC
-- MAGIC - One month is considered elapsed when the calendar month has increased and the calendar day and time is equal or greater to the `start`. Weeks, quarters, and years follow from that.

-- COMMAND ----------

-- DBTITLE 1,date_diff - Syntax
date_diff(unit, start, end)

unit
 { MICROSECOND |
   MILLISECOND |
   SECOND |
   MINUTE |
   HOUR |
   DAY |
   WEEK |
   MONTH |
   QUARTER |
   YEAR }

-- COMMAND ----------

-- DBTITLE 1,date_diff - Examples
-- One second shy of a month elapsed
SELECT date_diff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'); -- 0

-- One month has passed even though its' not end of the month yet because day and time line up.
SELECT date_diff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00'); -- 1

-- Start is greater than the end
SELECT date_diff(YEAR, DATE'2021-01-01', DATE'1900-03-28'); -- -120

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `date_format`
-- MAGIC
-- MAGIC Converts a timestamp to a string in the format `fmt`.
-- MAGIC
-- MAGIC #### Arguments
-- MAGIC
-- MAGIC - `expr`: A `DATE`, `TIMESTAMP`, or a `STRING` in a valid datetime format.
-- MAGIC - `fmt`: A `STRING` expression describing the desired format.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC A STRING.

-- COMMAND ----------

-- DBTITLE 1,date_format - Syntax
date_format(expr, fmt)

-- COMMAND ----------

-- DBTITLE 1,date_format - Example
SELECT date_format('2016-04-08', 'y'); -- 2016

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `date_part`
-- MAGIC
-- MAGIC Extracts a part of the date, timestamp, or interval.
-- MAGIC
-- MAGIC #### Arguments
-- MAGIC
-- MAGIC - `fieldStr`: An STRING literal.
-- MAGIC - `expr`: A `DATE`, `TIMESTAMP`, or `INTERVAL` expression.
-- MAGIC
-- MAGIC #### Returns
-- MAGIC
-- MAGIC If `fieldStr` is 'SECOND', a DECIMAL(8, 6). In all other cases, an INTEGER.
-- MAGIC
-- MAGIC Supported values of `field` when `source` is `DATE` or `TIMESTAMP`:
-- MAGIC
-- MAGIC - `'YEAR'`, `'Y'`, `'YEARS'`, `'YR'`, `'YRS'`: The year `field`
-- MAGIC - `'YEAROFWEEK'`: The ISO 8601 week-numbering year that the datetime falls in. For example, 2005-01-02 is part of the 53rd week of year 2004, so the result is 2004
-- MAGIC - `'QUARTER'`, `'QTR'`: The quarter (1 - 4) of the year that the datetime falls in
-- MAGIC - `'MONTH'`, `'MON'`, `'MONS'`, `'MONTHS'`: The month `field` (1 - 12)
-- MAGIC - `'WEEK'`, `'W'`, `'WEEKS'`: The number of the ISO 8601 week-of-week-based-year. A week is considered to start on a Monday and week 1 is the first week with >3 days. In the ISO week-numbering system, it is possible for early-January dates to be part of the 52nd or 53rd week of the previous year, and for late-December dates to be part of the first week of the next year. For example, 2005-01-02 is part of the 53rd week of year 2004, while 2012-12-31 is part of the first week of 2013
-- MAGIC - `'DAY'`, `'D'`, `'DAYS'`: The day of the month `field` (1 - 31)
-- MAGIC - `'DAYOFWEEK'`, `'DOW'`: The day of the week for datetime as Sunday(1) to Saturday(7)
-- MAGIC - `'DAYOFWEEK_ISO'`, `'DOW_ISO'`: ISO 8601 based day of the week for datetime as Monday(1) to Sunday(7)
-- MAGIC - `'DOY'`: The day of the year (1 - 365/366)
-- MAGIC - `'HOUR'`, `'H'`, `'HOURS'`, `'HR'`, `'HRS'`: The hour `field` (0 - 23)
-- MAGIC - `'MINUTE'`, `'M'`, `'MIN'`, `'MINS'`, `'MINUTES'`: The minutes `field` (0 - 59)
-- MAGIC - `'SECOND'`, `'S'`, `'SEC'`, `'SECONDS'`, `'SECS'`: The seconds `field`, including fractional parts
-- MAGIC
-- MAGIC Supported values of `field` when `source` is `INTERVAL` are (case-insensitive):
-- MAGIC - `'YEAR'`, `'Y'`, `'YEARS'`, `'YR'`, `'YRS'`: The total months / 12
-- MAGIC - `'MONTH'`, `'MON'`, `'MONS'`, `'MONTHS'`: The total months % 12
-- MAGIC - `'DAY'`, `'D'`, `'DAYS'`: The days part of interval
-- MAGIC - `'HOUR'`, `'H'`, `'HOURS'`, `'HR'`, `'HRS'`: How many hours the microseconds contains
-- MAGIC - `'MINUTE'`, `'M'`, `'MIN'`, `'MINS'`, `'MINUTES'`: How many minutes left after taking hours from microseconds
-- MAGIC - `'SECOND'`, `'S'`, `'SEC'`, `'SECONDS'`, `'SECS'`: How many seconds with fractions left after taking hours and minutes from microseconds
-- MAGIC
-- MAGIC The `date_part` function is a synonym for the SQL standard `extract` function.
-- MAGIC
-- MAGIC For example `date_part('year', CURRENT_DATE)` is equivalent to `extract(YEAR FROM CURRENT_DATE)`

-- COMMAND ----------

-- DBTITLE 1,date_part - Syntax
date_part(fieldStr, expr)

-- COMMAND ----------

-- DBTITLE 1,date_part - Examples
SELECT date_part('YEAR', TIMESTAMP'2019-08-12 01:00:00.123456'); -- 2019

SELECT date_part('Week', TIMESTAMP'2019-08-12 01:00:00.123456'); -- 33

SELECT date_part('day', DATE'2019-08-12'); -- 12

SELECT date_part('SECONDS', TIMESTAMP'2019-10-01 00:00:01.000001'); -- 1.000001

SELECT date_part('Months', INTERVAL '2-11' YEAR TO MONTH); -- 11

SELECT date_part('seconds', INTERVAL '5:00:30.001' HOUR TO SECOND); -- 30.001000