-- View: DEV_DBM.INGESTION_METADATA.VW_METADATA
-- Authoritative metadata source replacing DEV_PAYMENTS_BRONZE_RAW.ZAPP_PAYMENT_METADATA.PAYMENTMETADATA
-- Assumptions:
-- - One database per data feed (mt.DatabaseName)
-- - Two schemas per database: RAW and TARGET
-- - Stage naming convention: @<DBNAME>.RAW.STG_<DataFeedName>
-- - File format located in <DBNAME>.RAW using name from FILEFORMAT table

CREATE OR REPLACE VIEW DEV_DBM.INGESTION_METADATA.VW_METADATA AS
SELECT
    mt.DatabaseName          AS DBNAME,
    mt.SchemaName            AS SCHEMANAME,
    mt.TableName             AS TABLENAME,
    mf.FieldName             AS COLUMNNAME,
    UPPER(mf.FieldDataType)  AS DATATYPE,
    mf.IsPrimaryKey          AS PRIMARYKEYFLAG,
    CAST(NULL AS BOOLEAN)    AS COLUMNFORLASTMODIFIEDDATEFLAG,
    COALESCE(mf.ROW_MOD_DT, mt.ROW_MOD_DT) AS CREATEDDATE,
    mf.OrdinaryPosition      AS ORDINAL_POSITION,
    ff.FileFormatName        AS FILEFORMAT_NAME,
    (mt.DatabaseName || '.RAW.' || ff.FileFormatName)         AS FILE_FORMAT_FQN,
    df.DataFeedName          AS DATAFEED_NAME,
    ('STG_' || df.DataFeedName)                                AS STAGE_NAME,
    ('@' || mt.DatabaseName || '.RAW.' || 'STG_' || df.DataFeedName) AS STAGE_FQN,
    (mt.DatabaseName || '/' || mt.SchemaName || '/' || mt.TableName || '/') AS PATH
FROM DEV_DBM.INGESTION_METADATA.METADATATABLE mt
JOIN DEV_DBM.INGESTION_METADATA.METADATAFIELD mf
  ON mf.TableId = mt.TableId
JOIN DEV_DBM.INGESTION_METADATA.DATAFEED df
  ON df.DataFeedId = mt.DataFeedId
LEFT JOIN DEV_DBM.INGESTION_METADATA.FILEFORMAT ff
  ON mt.FileFormatId = ff.FileFormatId
WHERE mf.IsActive = TRUE;