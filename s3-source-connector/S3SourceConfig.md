## aws\.access\.key\.id
- Default value: null
- Type: PASSWORD
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.NonEmptyPassword@451d6bc9
- Importance: MEDIUM

AWS Access Key ID

## aws\.credentials\.provider
- Default value: class com\.amazonaws\.auth\.DefaultAWSCredentialsProviderChain
- Type: CLASS
- Valid values: null
- Importance: MEDIUM

When you initialize a new service client without supplying any arguments, the AWS SDK for Java attempts to find temporary credentials by using the default credential provider chain implemented by the DefaultAWSCredentialsProviderChain class.

## aws\.s3\.backoff\.delay\.ms
- Default value: 100
- Type: LONG
- Valid values: \[1,\.\.\.\]
- Importance: MEDIUM

S3 default base sleep time for non-throttled exceptions in milliseconds. Default is 100.

## aws\.s3\.backoff\.max\.delay\.ms
- Default value: 20000
- Type: LONG
- Valid values: \[1,\.\.\.\]
- Importance: MEDIUM

S3 maximum back-off time before retrying a request in milliseconds. Default is 20000.

## aws\.s3\.backoff\.max\.retries
- Default value: 3
- Type: INT
- Valid values: \[1,\.\.\.,30\]
- Importance: MEDIUM

Maximum retry limit (if the value is greater than 30, there can be integer overflow issues during delay calculation). Default is 3.

## aws\.s3\.bucket\.name
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$BucketNameValidator@5c936b5f
- Importance: MEDIUM

AWS S3 Bucket name

## aws\.s3\.endpoint
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.UrlValidator@5c2fc86
- Importance: LOW

Explicit AWS S3 Endpoint Address, mainly for testing

## aws\.s3\.fetch\.buffer\.size
- Default value: 1000
- Type: INT
- Valid values: \[1,\.\.\.\]
- Importance: MEDIUM

AWS S3 Fetch buffer size, this is the number of s3object keys kept in a buffer to ensure lexically older objet keys aren't skipped for processing if they are slower to upload.

## aws\.s3\.fetch\.page\.size
- Default value: 10
- Type: INT
- Valid values: \[1,\.\.\.\]
- Importance: MEDIUM

AWS S3 Fetch page size

## aws\.s3\.prefix
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

Prefix for stored objects, e.g. cluster-1/

## aws\.s3\.region
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$AwsRegionValidator@2baf9cd4
- Importance: MEDIUM

AWS S3 Region, e.g. us-east-1

## aws\.s3\.sse\.algorithm
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

AWS S3 Server Side Encryption Algorithm. Example values: 'AES256', 'aws:kms'.

## aws\.secret\.access\.key
- Default value: null
- Type: PASSWORD
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.NonEmptyPassword@50c0392d
- Importance: MEDIUM

AWS Secret Access Key

## aws\.sts\.config\.endpoint
- Default value: https://sts\.amazonaws\.com
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

AWS STS Config Endpoint

## aws\.sts\.role\.arn
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

AWS STS Role

## aws\.sts\.role\.external\.id
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

AWS STS External Id

## aws\.sts\.role\.session\.duration
- Default value: 3600
- Type: INT
- Valid values: \[900,\.\.\.,43200\]
- Importance: MEDIUM

AWS STS Session duration

## aws\.sts\.role\.session\.name
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

AWS STS Session name

## aws\_access\_key\_id
- Default value: null
- Type: PASSWORD
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$1@174e79f9
- Importance: MEDIUM

AWS Access Key ID

## aws\_s3\_bucket
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$3@57a815a
- Importance: MEDIUM

AWS S3 Bucket name

## aws\_s3\_endpoint
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$4@5264e41d
- Importance: LOW

Explicit AWS S3 Endpoint Address, mainly for testing

## aws\_s3\_prefix
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

Prefix for stored objects, e.g. cluster-1/

## aws\_s3\_region
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$5@3bca62d
- Importance: MEDIUM

AWS S3 Region, e.g. us-east-1

## aws\_secret\_access\_key
- Default value: null
- Type: PASSWORD
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$2@67d99612
- Importance: MEDIUM

AWS Secret Access Key

## distribution\.type
- Default value: OBJECT\_HASH
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.SourceConfigFragment$ObjectDistributionStrategyValidator@36f8abe2
- Importance: MEDIUM

Based on tasks.max config and the type of strategy selected, objects are processed in distributed way by Kafka connect workers, supported values : object_hash, partition

## errors\.tolerance
- Default value: NONE
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.SourceConfigFragment$ErrorsToleranceValidator@4349f52c
- Importance: MEDIUM

Indicates to the connector what level of exceptions are allowed before the connector stops, supported values : none,all

## expected\.max\.message\.bytes
- Default value: 1048588
- Type: INT
- Valid values: null
- Importance: MEDIUM

The largest record batch size allowed by Kafka config max.message.bytes

## file\.compression\.type
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.FileCompressionTypeValidator@2522bea6
- Importance: MEDIUM

The compression type used for files put on S3. The supported values are: 'none', 'gzip', 'snappy', 'zstd'.

## file\.max\.records
- Default value: 0
- Type: INT
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.FileNameFragment$1@e60c516
- Importance: MEDIUM

The maximum number of records to put in a single file. Must be a non-negative integer number. 0 is interpreted as "unlimited", which is the default.

## file\.name\.template
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.FilenameTemplateValidator@7db4c883
- Importance: MEDIUM

The template for file names on S3. Supports `{{ variable }}` placeholders for substituting variables. Currently supported variables are `topic`, `partition`, and `start_offset` (the offset of the first record in the file). Only some combinations of variables are valid, which currently are:
- `topic`, `partition`, `start_offset`.There is also `key` only variable {{key}} for grouping by keys

## file\.name\.timestamp\.source
- Default value: WALLCLOCK
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.TimestampSourceValidator@7158a28c
- Importance: LOW

Specifies the the timestamp variable source. Default is wall-clock.

## file\.name\.timestamp\.timezone
- Default value: Z
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.TimeZoneValidator@413fe58f
- Importance: LOW

Specifies the timezone in which the dates and time for the timestamp variable will be treated. Use standard shot and long names. Default is UTC

## format\.output\.envelope
- Default value: true
- Type: BOOLEAN
- Valid values: null
- Importance: MEDIUM

Whether to enable envelope for entries with single field.

## format\.output\.fields
- Default value: \[value\]
- Type: LIST
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.OutputFieldsValidator@49ffd0cb
- Importance: MEDIUM

Fields to put into output files. The supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'.

## format\.output\.fields\.value\.encoding
- Default value: base64
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.OutputFieldsEncodingValidator@1254e9a7
- Importance: MEDIUM

The type of encoding for the value field. The supported values are: 'none', 'base64'.

## format\.output\.type
- Default value: csv
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.validators\.OutputTypeValidator@262c75a3
- Importance: MEDIUM

The format type of output contentThe supported values are: 'avro', 'csv', 'json', 'jsonl', 'parquet'.

## input\.format
- Default value: bytes
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

Input format of messages read from source avro/json/parquet/bytes

## max\.poll\.records
- Default value: 500
- Type: INT
- Valid values: \[1,\.\.\.\]
- Importance: MEDIUM

Max poll records

## output\_compression
- Default value: null
- Type: STRING
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$8@1881ef74
- Importance: MEDIUM

Output compression. Valid values are: gzip and none

## output\_fields
- Default value: null
- Type: LIST
- Valid values: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$7@2fcf1d27
- Importance: MEDIUM

Output fields. A comma separated list of one or more: key, offset, timestamp, value, headers

## schema\.registry\.url
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

SCHEMA REGISTRY URL

## topics
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

eg : connect-storage-offsets

## transformer\.max\.buffer\.size
- Default value: 4096
- Type: INT
- Valid values: io\.aiven\.kafka\.connect\.common\.config\.TransformerFragment$ByteArrayTransformerMaxBufferSizeValidator@1ae3d0a8
- Importance: MEDIUM

Max Size of the byte buffer when using the BYTE Transformer

## value\.converter\.schema\.registry\.url
- Default value: null
- Type: STRING
- Valid values: non\-empty string
- Importance: MEDIUM

SCHEMA REGISTRY URL

## value\.serializer
- Default value: null
- Type: CLASS
- Valid values: null
- Importance: MEDIUM

Avro value serializer
