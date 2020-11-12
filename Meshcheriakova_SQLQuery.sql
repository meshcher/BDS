CREATE LOGIN meshcher 
    WITH PASSWORD = '****'

CREATE SCHEMA mesh_schema 
GO
CREATE USER [meshcheriakova] WITH PASSWORD=N'***', DEFAULT_SCHEMA=[mesh_schema]
GO
EXEC sp_addrolemember N'db_owner', N'meshcheriakova'

CREATE DATABASE SCOPED CREDENTIAL AZURE_CRED4
WITH
  IDENTITY='SHARED ACCESS SIGNATURE',
 SECRET='***';

CREATE EXTERNAL DATA SOURCE ext_dat_meshcher7
WITH
  (  LOCATION = 'wasbs://meshcheriakovao@meshcherbds.blob.core.windows.net',
     
     CREDENTIAL = AZURE_CRED4, 
    
    TYPE = HADOOP 
     );

CREATE EXTERNAL FILE FORMAT CSVF
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS ( FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2   )
);
GO

CREATE EXTERNAL TABLE yt_ext
(
    	[VendorID] int NULL,
	[tpep_pickup_datetime] datetime NOT NULL,
   	[tpep_dropoff_datetime] datetime NOT NULL,
	[passenger_count] int NULL,
	[Trip_distance] float NOT NULL,
	[RatecodeID] int NULL,
    	[store_and_fwd_flag] char(1)  NULL,
	[PULocationID] int NOT NULL,
	[DOLocationID] int NOT NULL,
	[payment_type] float NULL,
	[fare_amount] float  NOT NULL,
	[extra] float NOT NULL,
	[mta_tax] float NOT NULL,
	[tip_amount] float NOT NULL,
	[tolls_amount] float NOT NULL,
	[improvement_surcharge] float NOT NULL,
	[total_amount] float NOT NULL,
	[congestion_surcharge] float NOT NULL
)
WITH (
	
    LOCATION = '/yellow_tripdata_2020-01.csv',
    DATA_SOURCE = ext_dat_meshcher7,
    FILE_FORMAT = CSVF
);

CREATE TABLE mesh_schema.mesh_yellow_trip2
(   [VendorID] int NULL,
	[tpep_pickup_datetime] datetime NOT NULL,
   	[tpep_dropoff_datetime] datetime NOT NULL,
	[passenger_count] int NULL,
	[Trip_distance] float NOT NULL,
	[RatecodeID] int NULL,
    [store_and_fwd_flag] char(1)  NULL,
	[PULocationID] int NOT NULL,
	[DOLocationID] int NOT NULL,
	[payment_type] float NULL,
	[fare_amount] float  NOT NULL,
	[extra] float NOT NULL,
	[mta_tax] float NOT NULL,
	[tip_amount] float NOT NULL,
	[tolls_amount] float NOT NULL,
	[improvement_surcharge] float NOT NULL,
	[total_amount] float NOT NULL,
	[congestion_surcharge] float NOT NULL
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH(tpep_dropoff_datetime)
)
;





CREATE TABLE mesh_schema.vendor
(
    [ID] int NULL,
    [NAME] char(100) NULL
)

WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH(ID)
)
;

INSERT INTO mesh_schema.vendor2
([ID],[NAME])
SELECT 1,'Creative Mobile Technologies,LLC'
UNION ALL
SELECT 2,'VeriFone Inc.'



CREATE TABLE mesh_schema.ratecode
(
    [ID] int NULL,
    [NAME] char(100) NULL
)

WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH(ID)
)
;

INSERT INTO mesh_schema.ratecode
([ID],[NAME])
SELECT 1,'Standard rate'
UNION ALL
SELECT 2,'JFK'
UNION ALL
SELECT 3,'Newark'
UNION ALL
SELECT 4,'Nassau or Westchester'
UNION ALL
SELECT 5,'Negotiated fare'
UNION ALL
SELECT 6,'Group ride'

CREATE TABLE mesh_schema.payment_type
(
    [ID] int NULL,
    [NAME] char(100) NULL
)

WITH
(   CLUSTERED COLUMNSTORE INDEX
,  DISTRIBUTION = HASH(ID)
)
;

INSERT INTO mesh_schema.payment_type
([ID],[NAME])
SELECT 1,'Credit card'
UNION ALL
SELECT 2,'Cash'
UNION ALL
SELECT 3,'No charge'
UNION ALL
SELECT 4,'Dispute'
UNION ALL
SELECT 5,'Unknown'
UNION ALL
SELECT 6,'Voided trip'


