CREATE EXTERNAL TABLE :'schema_name'.:'table_name'(
    "CVE_data_type"  varchar,
    "CVE_data_format"  varchar,
    "CVE_data_version"  varchar,
    "CVE_data_numberOfCVEs"  varchar,
    "CVE_data_timestamp"  varchar,
    "CVE_Items"  array<struct<"cve":struct<
    "data_type":varchar,
    "data_format":varchar,
    "data_version":varchar,
    "CVE_data_meta":struct<"ID":varchar, "ASSIGNER":varchar>,
    "problemtype":struct<"problemtype_data":array<struct<"description":array<struct<"lang": varchar, "value": varchar>>>>>,
    "references":struct<"reference_data":array<struct<"url": varchar, "name": varchar, "refsource": varchar, "tags": array<varchar>>>>,
    "description":struct<"description_data": array<struct<"lang": varchar, "value": varchar>>>,
    "configurations":struct<"CVE_data_version": varchar, "nodes": array<varchar>>>,
    "impact":struct<"baseMetricV3":struct<"cvssV3": struct<"version": varchar, "vectorString": varchar, "attackVector": varchar, "attackComplexity": varchar, "privilegesRequired": varchar, "userInteraction": varchar, "scope": varchar, "confidentialityImpact": varchar, "integrityImpact": varchar, "availabilityImpact": varchar, "baseScore": float, "baseSeverity": varchar>, "exploitabilityScore": float, "impactScore": float>>,
    "publishedDate":DATE,
    "lastModifiedDate":DATE>>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION :'s3_json_location'