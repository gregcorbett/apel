
-- ------------------------------------------------------------------------------
-- JobRecords
DROP TABLE IF EXISTS DataSetRecords;
CREATE TABLE DataSetRecords ( 
  UpdateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 

  -- Record Identity Block
  RecordId             VARCHAR(255) NOT NULL PRIMARY KEY,
  CreateTime           DATETIME NOT NULL,
  ResourceProvider     VARCHAR(255) NOT NULL, -- Resource provider at which
                                              -- the resource is located 
                                              -- (e.g. GOCDB sitename)
  Infrastructure       VARCHAR(255) NOT NULL, -- The high level entity, i.e. datahub.egi.eu
  

  -- Subject Identity Block
  GlobalUserId         VARCHAR(255), -- e.g. X.509 certificate DN or
                                     -- EGI unique ID (from Checkin service)
  GlobalGroupId        VARCHAR(255), -- e.g. VO
  ORCID                VARCHAR(255), -- ORCID of the user

  -- DataSet Usage Block
  DataSetID            VARCHAR(255), -- unique identifier such as a PI / DOI
  DataSetIDType        VARCHAR(255), -- type of unique identifier, i.e DOI, OneData Share etc
  ReadAccessEvents     INT, -- Number of read operations
  WriteAccessEvents    INT, -- Number of write operations
  Source               VARCHAR(255), -- Source of transfer at resource provider
  Destination          VARCHAR(255), -- Destination of transfer
  StartTime            DATETIME, -- Start time of transfer
  Duration             BIGINT, -- Duration of transfer
  EndTime              DATETIME, -- End time of transfer
  TransferSize         INT, -- bytes transfered
  HostType             VARCHAR(255), -- Storage system Type
  FileCount            INT, -- Number of files accessed
  Status               VARCHAR(255) -- Success / failure / partial transfer
);

DROP PROCEDURE IF EXISTS ReplaceDataSetRecord;
DELIMITER //
CREATE PROCEDURE ReplaceDataSetRecord(
  recordid VARCHAR(255), createtime DATETIME, resourceprovider VARCHAR(255),
  infrastructure VARCHAR(255), globaluserid VARCHAR(255),
  globalgroupid VARCHAR(255), orchid VARCHAR(255),
  datasetid VARCHAR(255), datasetidtype VARCHAR(255), readaccessevents INT,
  writeaccessevents INT, source VARCHAR(255), destination VARCHAR(255),
  starttime DATETIME, duration BIGINT, endtime DATETIME, transfersize INT,
  hosttype VARCHAR(255), filecount INT, status VARCHAR(255))
BEGIN
    REPLACE INTO DataSetRecords(
      RecordId, CreateTime, ResourceProvider, Infrastructure,
      GlobalUserId, GlobalGroupId, ORCID,
      DataSetID, DataSetIDType, ReadAccessEvents,
      WriteAccessEvents, Source, Destination,
      StartTime, Duration, EndTime, TransferSize,
      HostType, FileCount, Status
    )
    VALUES (
      recordid, createtime, resourceprovider,
      infrastructure, globaluserid,
      globalgroupid, orchid,
      datasetid, datasetidtype, readaccessevents,
      writeaccessevents, source, destination,
      starttime, duration, endtime, transfersize,
      hosttype, filecount, status
    );
END //
DELIMITER ;

DROP TABLE IF EXISTS DataSetSummaries;
CREATE TABLE DataSetSummaries (
  UpdateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  ResourceProvider        VARCHAR(255) NOT NULL, -- Resource provider at which
                                                 -- the resource is located
                                                 -- (e.g. GOCDB sitename)

  Infrastructure          VARCHAR(255), -- -- The high level entity, i.e. datahub.egi.eu
  GlobalUserId            VARCHAR(255), -- e.g. X.509 certificate DN or
                                        -- EGI unique ID (from Checkin service)
  GlobalGroupId           VARCHAR(255), -- e.g. VO
  ORCID                   VARCHAR(255), -- ORCID of the user

  DataSetID               VARCHAR(255), -- unique identifier such as a PI / DOI
  DataSetIDType           VARCHAR(255), -- type of unique identifier, i.e DOI, OneData Share etc
  TotalReadAccessEvents   BIGINT, -- Number of read operations
  TotalWriteAccessEvents  BIGINT, -- Number of write operations
  Source                  VARCHAR(255), -- Source of transfer at resource provider
  Destination             VARCHAR(255), -- Destination of transfer
  EarliestStartTime       DATETIME, -- Start time of transfer
  TotalDuration           BIGINT, -- Duration of transfer
  LatestStartTime         DATETIME, -- End time of transfer
  Month                   INT NOT NULL,
  Year                    INT NOT NULL,
  TotalTransferSize       BIGINT, -- bytes transfered
  HostType                VARCHAR(255), -- Storage system Type
  TotalFileCount          BIGINT, -- Number of files accessed
  Status                  VARCHAR(255) -- Success / failure / partial transfer
);

DROP PROCEDURE IF EXISTS SummariseDataSets;
DELIMITER //
CREATE PROCEDURE SummariseDataSets()
BEGIN
    REPLACE INTO DataSetSummaries(
      ResourceProvider, Infrastructure, GlobalUserId, GlobalGroupId, ORCID,
      DataSetID, DataSetIDType, TotalReadAccessEvents, TotalWriteAccessEvents,
      Source, Destination, EarliestStartTime, TotalDuration, LatestStartTime,
      Month, Year, TotalTransferSize, HostType, TotalFileCount, Status)
    SELECT
      ResourceProvider,
      Infrastructure,
      GlobalUserId,
      GlobalGroupId,
      ORCID,
      DataSetID,
      DataSetIDType,
      SUM(ReadAccessEvents),
      SUM(WriteAccessEvents),
      Source,
      Destination,
      MIN(StartTime),
      SUM(Duration),
      MAX(StartTime),
      MONTH(StartTime) AS Month,
      YEAR(StartTime) AS Year,
      SUM(TransferSize),
      HostType,
      SUM(FileCount),
      Status
    FROM DataSetRecords
    GROUP BY ResourceProvider, Infrastructure, GlobalUserId, GlobalGroupId, ORCID,
      DataSetID, DataSetIDType, Source, Destination, HostType, Status 
    ORDER BY NULL;
END //
DELIMITER ;

-- ------------------------------------------------------------------------------
-- GroupAttributes
DROP TABLE IF EXISTS GroupAttributes;
CREATE TABLE GroupAttributes (
    RecordId                VARCHAR(255) NOT NULL,
    AttributeType           VARCHAR(255),
    AttributeValue          VARCHAR(255),
    PRIMARY KEY(RecordId, AttributeType)
    );

DROP PROCEDURE IF EXISTS ReplaceGroupAttribute;
DELIMITER //
CREATE PROCEDURE ReplaceGroupAttribute(
    RecordId                VARCHAR(255),
    attributeType           VARCHAR(255),
    attributeValue          VARCHAR(255)
    )
BEGIN
    REPLACE INTO GroupAttributes(RecordId, AttributeType, AttributeValue)
    VALUES (RecordId, attributeType, attributeValue);
END //
DELIMITER ;

-- ------------------------------------------------------------------------------
-- LastUpdated
DROP TABLE IF EXISTS LastUpdated;
CREATE TABLE LastUpdated (
  UpdateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  Type VARCHAR(255) PRIMARY KEY
);

DROP PROCEDURE IF EXISTS UpdateTimestamp;
DELIMITER //
CREATE PROCEDURE UpdateTimestamp(type VARCHAR(255)) 
  BEGIN
   REPLACE INTO LastUpdated (Type) VALUES (type);
  END //

DELIMITER ; 
