COPY fact_trips
FROM 's3://transformed-taxi-data'
IAM_ROLE 'arn:aws:iam::176256382487:role/taxi-trips-RedshiftAccessDataRole-UA119OI71PGW'
FORMAT AS CSV
IGNOREHEADER 1
IGNOREBLANKLINES;
