-- Procedure: DEV_DBM.INGESTION_METADATA.UPDATE_TABLE_METADATA
-- Replaces legacy metadata-table driven flow with VW_METADATA
-- Ensures RAW/TARGET table existence and structure; creates pipe/stream/task as needed

CREATE OR REPLACE PROCEDURE DEV_DBM.INGESTION_METADATA.UPDATE_TABLE_METADATA(
  DBNAME VARCHAR,
  SCHEMANAME VARCHAR,
  TABLENAMEINPUT VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  action_needed STRING;
  v_sql STRING;
  v_target_sql STRING;
  v_exists INT;
BEGIN
  CALL DEV_DBM.INGESTION_METADATA.CREATE_SCHEMAS_IF_NOT_EXISTS(:DBNAME);

  -- Determine if RAW table exists
  EXECUTE IMMEDIATE ''SHOW TABLES LIKE ''''RAW_''||:TABLENAMEINPUT||'''''' IN SCHEMA ''||:DBNAME||''.RAW'';
  SELECT COUNT(*) INTO :v_exists FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  IF (NVL(:v_exists,0) = 0) THEN
    -- NEW: create raw and target tables and all ingestion artifacts
    CALL DEV_DBM.INGESTION_METADATA.CREATE_NEW_TABLES(:DBNAME, :SCHEMANAME, :TABLENAMEINPUT, ''NEW'');
    RETURN ''Successful'';
  END IF;

  -- ADD/MODIFY columns if needed based on VW_METADATA vs current info schema
  WITH current_raw AS (
    SELECT column_name, data_type, ordinal_position
    FROM "'||:DBNAME||'".INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = ''RAW'' AND table_name = ''RAW_''||:TABLENAMEINPUT
  ), current_target AS (
    SELECT column_name, data_type, ordinal_position
    FROM "'||:DBNAME||'".INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = ''TARGET'' AND table_name = :TABLENAMEINPUT
  ), desired AS (
    SELECT COLUMNNAME AS column_name,
           CASE WHEN UPPER(DATATYPE) IN (''BIGINT'',''INT'') THEN ''INT''
                WHEN UPPER(DATATYPE) = ''BIT'' THEN ''BOOLEAN''
                WHEN UPPER(DATATYPE) = ''MONEY'' THEN ''NUMBER''
                WHEN UPPER(DATATYPE) = ''CHAR'' THEN ''STRING''
                ELSE UPPER(DATATYPE) END AS data_type,
           ORDINAL_POSITION
    FROM DEV_DBM.INGESTION_METADATA.VW_METADATA
    WHERE UPPER(DBNAME)=UPPER(:DBNAME) AND UPPER(SCHEMANAME)=UPPER(:SCHEMANAME) AND UPPER(TABLENAME)=UPPER(:TABLENAMEINPUT)
  ), diff_raw AS (
    SELECT d.* FROM desired d LEFT JOIN current_raw c USING (column_name)
    WHERE c.column_name IS NULL
  ), diff_target AS (
    SELECT d.* FROM desired d LEFT JOIN current_target c USING (column_name)
    WHERE c.column_name IS NULL
  )
  SELECT CASE WHEN COUNT(*) > 0 THEN ''ADD'' ELSE ''NONE'' END INTO :action_needed FROM diff_raw;

  IF (:action_needed = ''ADD'') THEN
    SELECT 'ALTER TABLE '||:DBNAME||'.RAW.RAW_'||:TABLENAMEINPUT||' ADD COLUMN '||LISTAGG(column_name||' '||data_type, ', ') WITHIN GROUP (ORDER BY ordinal_position)
      INTO :v_sql FROM diff_raw;
    EXECUTE IMMEDIATE :v_sql;
  END IF;

  SELECT CASE WHEN COUNT(*) > 0 THEN ''ADD'' ELSE ''NONE'' END INTO :action_needed FROM diff_target;
  IF (:action_needed = ''ADD'') THEN
    SELECT 'ALTER TABLE '||:DBNAME||'.TARGET.'||:TABLENAMEINPUT||' ADD COLUMN '||LISTAGG(column_name||' '||CASE WHEN data_type = ''STRING'' THEN ''VARCHAR'' ELSE data_type END, ', ') WITHIN GROUP (ORDER BY ordinal_position)
      INTO :v_target_sql FROM diff_target;
    EXECUTE IMMEDIATE :v_target_sql;
  END IF;

  -- Ensure dependent objects exist
  CALL DEV_DBM.INGESTION_METADATA.CREATE_NEW_TABLES(:DBNAME, :SCHEMANAME, :TABLENAMEINPUT, ''ADD'');
  CALL DEV_DBM.INGESTION_METADATA.CHECK_TASKS_EXIST(:DBNAME, :SCHEMANAME, :TABLENAMEINPUT);

  RETURN ''Successful'';
END';