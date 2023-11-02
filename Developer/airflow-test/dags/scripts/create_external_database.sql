CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE 'spectrumdb' iam_role 'arn:aws:iam::{{params.AWS_ID}}:role/{{params.IAM_ROLE_NAME}}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
