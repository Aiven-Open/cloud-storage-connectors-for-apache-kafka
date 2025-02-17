
## aws\.access\.key\.id
- Default value: null
- Type: PASSWORD
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.NonEmptyPassword@7080e8fc

AWS Access Key ID

## aws\.credentials\.provider
- Default value: class com\.amazonaws\.auth\.DefaultAWSCredentialsProviderChain
- Type: CLASS
- Validation: null

When you initialize a new service client without supplying any arguments, the AWS SDK for Java attempts to find temporary credentials by using the default credential provider chain implemented by the DefaultAWSCredentialsProviderChain class.

## aws\.s3\.backoff\.delay\.ms
- Default value: 100
- Type: LONG
- Validation: \[1,\.\.\.\]

S3 default base sleep time for non-throttled exceptions in milliseconds. Default is 100.

## aws\.s3\.backoff\.max\.delay\.ms
- Default value: 20000
- Type: LONG
- Validation: \[1,\.\.\.\]

S3 maximum back-off time before retrying a request in milliseconds. Default is 20000.

## aws\.s3\.backoff\.max\.retries
- Default value: 3
- Type: INT
- Validation: \[1,\.\.\.,30\]

Maximum retry limit (if the value is greater than 30, there can be integer overflow issues during delay calculation). Default is 3.

## aws\.s3\.bucket\.name
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$BucketNameValidator@25b8a2f7

AWS S3 Bucket name

## aws\.s3\.endpoint
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.UrlValidator@9a3d0f4

Explicit AWS S3 Endpoint Address, mainly for testing

## aws\.s3\.fetch\.page\.size
- Default value: 10
- Type: INT
- Validation: \[1,\.\.\.\]

AWS S3 Fetch page size

## aws\.s3\.prefix
- Default value: null
- Type: STRING
- Validation: non\-empty string

Prefix for stored objects, e.g. cluster-1/

## aws\.s3\.region
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$AwsRegionValidator@7e130e34

AWS S3 Region, e.g. us-east-1

## aws\.s3\.sse\.algorithm
- Default value: null
- Type: STRING
- Validation: non\-empty string

AWS S3 Server Side Encryption Algorithm. Example values: 'AES256', 'aws:kms'.

## aws\.secret\.access\.key
- Default value: null
- Type: PASSWORD
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.NonEmptyPassword@77d5c4c6

AWS Secret Access Key

## aws\.sts\.config\.endpoint
- Default value: https://sts\.amazonaws\.com
- Type: STRING
- Validation: non\-empty string

AWS STS Config Endpoint

## aws\.sts\.role\.arn
- Default value: null
- Type: STRING
- Validation: non\-empty string

AWS STS Role

## aws\.sts\.role\.external\.id
- Default value: null
- Type: STRING
- Validation: non\-empty string

AWS STS External Id

## aws\.sts\.role\.session\.duration
- Default value: 3600
- Type: INT
- Validation: \[900,\.\.\.,43200\]

AWS STS Session duration

## aws\.sts\.role\.session\.name
- Default value: null
- Type: STRING
- Validation: non\-empty string

AWS STS Session name

## aws\_access\_key\_id
- Default value: null
- Type: PASSWORD
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$1@171a6fd1

AWS Access Key ID

## aws\_s3\_bucket
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$3@a929b6

AWS S3 Bucket name

## aws\_s3\_endpoint
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$4@66ade539

Explicit AWS S3 Endpoint Address, mainly for testing

## aws\_s3\_prefix
- Default value: null
- Type: STRING
- Validation: non\-empty string

Prefix for stored objects, e.g. cluster-1/

## aws\_s3\_region
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$5@3f07eede

AWS S3 Region, e.g. us-east-1

## aws\_secret\_access\_key
- Default value: null
- Type: PASSWORD
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$2@7b276a08

AWS Secret Access Key

## distribution\.type
- Default value: OBJECT\_HASH
- Type: STRING
- Validation: Must be one of: \[OBJECT\_HASH, PARTITION\]

Based on tasks.max config and the type of strategy selected, objects are processed in distributed way by Kafka connect workers, supported values : object_hash, partition

## errors\.tolerance
- Default value: NONE
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.SourceConfigFragment$ErrorsToleranceValidator@3b07b706

Indicates to the connector what level of exceptions are allowed before the connector stops, supported values : none,all

## expected\.max\.message\.bytes
- Default value: 1048588
- Type: INT
- Validation: null

The largest record batch size allowed by Kafka config max.message.bytes

## file\.compression\.type
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.FileCompressionTypeValidator@3b76dfa6

The compression type used for files put on S3. The supported values are: 'none', 'gzip', 'snappy', 'zstd'.

## file\.max\.records
- Default value: 0
- Type: INT
- Validation: io\.aiven\.kafka\.connect\.common\.config\.FileNameFragment$1@7b4185ea

The maximum number of records to put in a single file. Must be a non-negative integer number. 0 is interpreted as "unlimited", which is the default.

## file\.name\.template
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.FilenameTemplateValidator@196bc2c1

The template for file names on S3. Supports `{{ variable }}` placeholders for substituting variables. Currently supported variables are `topic`, `partition`, and `start_offset` (the offset of the first record in the file). Only some combinations of variables are valid, which currently are:
- `topic`, `partition`, `start_offset`.There is also `key` only variable {{key}} for grouping by keys

## file\.name\.timestamp\.source
- Default value: WALLCLOCK
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.TimestampSourceValidator@1fd2f96d

Specifies the the timestamp variable source. Default is wall-clock.

## file\.name\.timestamp\.timezone
- Default value: Z
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.TimeZoneValidator@12d49ede

Specifies the timezone in which the dates and time for the timestamp variable will be treated. Use standard shot and long names. Default is UTC

## file\.prefix\.template
- Default value: topics/\{\{topic\}\}/partition=\{\{partition\}\}/
- Type: STRING
- Validation: non\-empty string

The template for file prefix on S3. Supports `{{ variable }}` placeholders for substituting variables. Currently supported variables are `topic` and `partition` and are mandatory to have these in the directory structure.Example prefix : topics/{{topic}}/partition/{{partition}}/

## format\.output\.envelope
- Default value: true
- Type: BOOLEAN
- Validation: null

Whether to enable envelope for entries with single field.

## format\.output\.fields
- Default value: \[value\]
- Type: LIST
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.OutputFieldsValidator@65cd37bb

Fields to put into output files. The supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'.

## format\.output\.fields\.value\.encoding
- Default value: base64
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.OutputFieldsEncodingValidator@22f79c09

The type of encoding for the value field. The supported values are: 'none', 'base64'.

## format\.output\.type
- Default value: csv
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.common\.config\.validators\.OutputTypeValidator@2a6600ab

The format type of output contentThe supported values are: 'avro', 'csv', 'json', 'jsonl', 'parquet'.

## input\.format
- Default value: bytes
- Type: STRING
- Validation: non\-empty string

Input format of messages read from source avro/json/parquet/bytes

## max\.poll\.records
- Default value: 500
- Type: INT
- Validation: \[1,\.\.\.\]

Max poll records

## output\_compression
- Default value: null
- Type: STRING
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$8@1c724957

Output compression. Valid values are: gzip and none

## output\_fields
- Default value: null
- Type: LIST
- Validation: io\.aiven\.kafka\.connect\.config\.s3\.S3ConfigFragment$7@108ed58d

Output fields. A comma separated list of one or more: key, offset, timestamp, value, headers

## schema\.registry\.url
- Default value: null
- Type: STRING
- Validation: non\-empty string

SCHEMA REGISTRY URL

## topic\.partitions
- Default value: 0
- Type: STRING
- Validation: non\-empty string

eg : 0,1

## topics
- Default value: null
- Type: STRING
- Validation: non\-empty string

eg : connect-storage-offsets

## value\.converter\.schema\.registry\.url
- Default value: null
- Type: STRING
- Validation: non\-empty string

SCHEMA REGISTRY URL

## value\.serializer
- Default value: null
- Type: CLASS
- Validation: null

Avro value serializer
