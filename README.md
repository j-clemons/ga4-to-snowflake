# GA4 to Snowflake
This tool moves the GA4 BigQuery export files into Snowflake. 

The GCS to Snowflake portion of this tool leverages [sling-cli](https://github.com/slingdata-io/sling-cli)

## Data Flow Overview
1. Export tables into GCS
2. Load exported files from GCS into Snowflake
3. Remove exported files from GCS

# Required Files
This tool requires configuration yaml files to provide the replication details for both exporting in BigQuery and replication using sling.

## gcs.yaml
This is the primary config file that provides the details for each source. An example is provided below.

The path for where this config file is stored needs to be set in the `GA4_SNOWFLAKE_CONFIG` environment variable.

The `replicationScheme` setting instructs the tool to either replicate over a set date range or if set to `today` the prior day's daily export and the current date's intraday file.
```
projectID: [GCP Project ID]
schema: [Dataset where the GA4 exports are sent. Default is analytics_######]
timezone: [Timezone string. Ex America/Chicago]
exportStrategy: [daily, streaming, daily+streaming]

sources:
  daily:
    tablePrefix: events_
    bucket: [GCS bucket name]
    bucketSuffix: [Path inside of GCS bucket. Ex. 'staging/daily']
    fileFormat: json
    replicationScheme: [Acceptable values (today, range)]
    dateRangeStart: 20231128 # Must be formatted as YYYYMMDD
    dateRangeEnd: 20231128
    slingCfgPath: ./dailyCfg.yaml
  intraday:
    tablePrefix: events_intraday_
    bucket: 
    bucketSuffix: 
    fileFormat: json
    replicationScheme: [Acceptable values (today, range)]
    dateRangeStart: 20231128 # Must be formatted as YYYYMMDD
    dateRangeEnd: 20231128
    slingCfgPath: ./intradayCfg.yaml
```

## Sling Config yaml Example
This tool references sling config yaml files for the replication instructions for each stream.
Separate files for each source stream are required. Additional details on creating these files can be found in the [sling docs](https://docs.slingdata.io/sling-cli/running-tasks).

```
source:
  conn: GCS_BUCKET
  stream: gs://sling-bucket/staging/daily/

target:
  conn: SNOWFLAKE
  object: DAILY_EXPORT

mode: full-refresh
```

## ~/.sling/env.yaml
This file contains the environment details for sling. [Sling docs for additional details](https://docs.slingdata.io/sling-cli/environment#sling-env-file-env.yaml).

For this tool to function it can be as simple as the example below, which will work with the other yaml templates shown.
```
connections:

  GCS_BUCKET:
    type: gs
    bucket: [GCS bucket path]
    key_file: [Path to local service account credentials]

  SNOWFLAKE:
    type: snowflake
    user: [snowflake username]
    password: [snowflake password]
    account: [snowflake account string]
    database: [database target]
    schema: [schema target]
```
