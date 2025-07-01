INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain
);

CREATE OR REPLACE TABLE webapp AS
SELECT *
FROM read_ndjson(
    -- 's3://application-logs-b5893cd/application-logs/v1/edgar/api/*.gz',
    -- 's3://application-logs-fb0e951/application-logs/v1/arch/webapp/*.gz',
    's3://application-logs-fb0e951/application-logs/v1/arch/webapp/2025-06-10*.gz',
    ignore_errors = true
);

CREATE OR REPLACE TABLE api AS
SELECT *
FROM read_ndjson(
    -- 's3://application-logs-b5893cd/application-logs/v1/edgar/api/*.gz',
    -- 's3://application-logs-fb0e951/application-logs/v1/arch/webapp/*.gz',
    's3://application-logs-fb0e951/application-logs/v1/arch/api/2025-06-10*.gz',
    ignore_errors = true
);

select json.request from api where json.response is not null and json.response.status_code <> 200;
