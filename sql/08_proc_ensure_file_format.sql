-- Procedure: DEV_DBM.INGESTION_METADATA.ENSURE_FILE_FORMAT
-- Ensures a CSV file format exists in <DB>.RAW with standard options

CREATE OR REPLACE PROCEDURE DEV_DBM.INGESTION_METADATA.ENSURE_FILE_FORMAT(
  DBNAME VARCHAR,
  FILEFORMAT_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  fmt_count INT;
  show_sql STRING;
BEGIN
  -- Ensure RAW schema exists first
  EXECUTE IMMEDIATE ''CREATE SCHEMA IF NOT EXISTS ''||:DBNAME||''.RAW'';

  -- Check if file format exists
  show_sql := ''SHOW FILE FORMATS IN SCHEMA ''||:DBNAME||''.RAW'';
  EXECUTE IMMEDIATE :show_sql;
  SELECT COUNT(*) INTO :fmt_count
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE UPPER("name") = UPPER(:FILEFORMAT_NAME);

  IF (NVL(:fmt_count,0) = 0) THEN
    EXECUTE IMMEDIATE ''CREATE FILE FORMAT ''||:DBNAME||''.RAW.''||:FILEFORMAT_NAME||''\n''
      || '' TYPE = CSV\n''
      || '' PARSE_HEADER = TRUE\n''
      || '' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE\n''
      || '' FIELD_OPTIONALLY_ENCLOSED_BY = ''''"''''\n''
      || '' ESCAPE = ''\''\n''
      || '' NULL_IF = ('''''''', ''''NULL'''', ''''null'''')'';
  END IF;
  RETURN ''OK'';
END';