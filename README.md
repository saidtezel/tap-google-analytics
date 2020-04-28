# Tap Google Analytics

This is a [Singer](https://singer.io) tap for extracting data from [Google Analytics API](https://developers.google.com/analytics/devguides/reporting/core/v4/).
This tap:

- Pulls raw data from the [Google Analytics Reporting API](https://developers.google.com/analytics/devguides/reporting/core/v4/).
- Supports generating as many reports as the user of the tap wants (following Google's limit of 7 dimensions and 10 metrics per report).
- Generates a valid Catalog that follows the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

As the Google Analytics Reports are defined dynamically and there are practically infinite combinations of dimensions and metrics a user can ask for, the entities and their schema (i.e. the Catalog for this tap) are not static. So, this tap behaves more or less similarly to a tap extracting data from a Data Source (e.g. a Postgres Database).

Based on the report(s) definition, it generates a valid Catalog that follows the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Additional Features

This tap also supports granular control over how the data is queried from Google Analytics.

### Daily/Weekly/Monthly Report Batching

This tap gives you the option to define batching config for the date ranges in your reports.

By default, it will query data from Google Analytics API in daily instances, regardless of whether you have defined a date column within your report definition. However, you can alter the batch sizes in the tap config using the `date_batching` key. This can be assigned as `DAY`, `WEEK` or `MONTH`. If it's omitted, the tap will do daily querying.

#### Example

Consider that you're defining a tap to pull date from 2018-01-01 to 2019-12-31.

**With daily batching:** The tap will make individual API requests for each day:

- First iteration: 2018-01-01 to 2018-01-01
- Second iteration: 2018-01-02 to 2018-01-02
- ...

**With weekly batching:** The tap will make individual API requests for every 7 days:

- First iteration: 2018-01-01 to 2018-01-07
- Second iteration: 2018-01-08 to 2018-01-14
- ...

**With monthly batching:** The tap will make individual API requests for every 30 days:

- First iteration: 2018-01-01 to 2018-01-30
- Second iteration: 2018-01-31 to 2018-03-01
- ...

In runtime, one exception to this configuration is when we are running incremental updates. If the total number of days defined in the report is less than 30, the tap will still query data based on daily batching.

Date batching for reports is particularly useful for reports with large date ranges, because it minimises the risk of data returned from Google Analytics API to be sampled, hence increasing the accuracy of returned data.

The fact that we are making separate queries for batched date ranges from the API also enables the opportunity to log the last day queried in the state, enabling us to run incremental queries in the tap. This way, if an error occurs during a single run, we can still pick up from where we left based on the stream log the next time we run it.

### Incremental Queries

This tap utilises Singer's [state functionality](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md) in order to keep a log of the last report date for each stream. This ensures that for reports with large date ranges, instead of replicating the data for the entire date range defined in report config, only the new dates are queried, hence significantly reducing the number of API calls necessary.

### Custom Sampling

The Google Analytics API provides option to query data with different sampling levels:
- `DEFAULT`: Balanced between speed and data accuracy.
- `SMALL`: A smaller sample of the data, which produces faster results but also lowers the data accuracy.
- `LARGE`: A larger sample of the data, which produces more accurate data but is also slower to process.

You have the option to select the desired sampling level for your reporting using an optional `sampling_level` key within the config file. If you don't define a sampling level, it will use the `DEFAULT` sampling level for queries.

### Lookback Period

Conversions in Google Analytics work based on a set attribution window to credit the correct traffic source for the conversion, so we need to account for any historical data that might have changed after we already pulled the data.

Since this tap has the option to work incrementally based on the last sync date, we provided the option to define a lookback window. This means that when we run this model incrementally, it will actually start the data pull 15 days prior to the starting point defined in the config or recorded in the log.

The lookback window optionally can be changed within the tap's config file, or be even set to 0.

It is, **however**, important to point out that since we are refetching data prior to the report start date, this will likely result in duplications on the target database.

### Segment Support

It is also possible to query data for a specific segment ID on Google Analytics. At the moment only one segment can be used for reporting, so if you are planning on querying data for different segments across a GA view, we recommend creating separate pipelines for each segment.

To enable segment support, you will need to make two changes in report configuration:

1. Update the tap config file and add a `segment_id` key with the segment ID as its value (gaid::xxxxx)
2. Within the report config file (where you define the report definitions) make sure to include `ga:segment` as a dimension. 

## Install the Tap

In a typical use case, where you install the Singer tap and a Singer target to work with, it is recommended to install each package in its own virtual enviroment. This is to eliminate the risk of dependency incompatibilities between the tap and target.

```
python -m venv env-tap
source env-tap/bin/activate
pip install git+https://github.com/saidtezel/tap-google-analytics.git
```
---
## Authorization Methods

`tap-google-analytics` supports two different ways of authorization:
 - Service account based authorization, where an administrator manually creates a service account with the appropriate permissions to view the account, property, and view you wish to fetch data from
 - OAuth `access_token` based authorization, where this tap gets called with a valid `access_token` and `refresh_token` produced by an OAuth flow conducted in a different system.

If you're setting up `tap-google-analytics` for your own organization and only plan to extract from a handful of different views in the same limited set of properties, Service Account based authorization is the simplest. When you create a service account Google gives you a json file with that service account's credentials called the `client_secrets.json`, and that's all you need to pass to this tap, and you only have to do it once, so this is the recommended way of configuring `tap-google-analytics`.

If you're building something where a wide variety of users need to be able to give access to their Google Analytics, `tap-google-analytics` can use an `access_token` granted by those users to authorize it's requests to Google. This `access_token` is produced by a normal Google OAuth flow, but this flow is outside the scope of `tap-google-analytics`. This is useful if you're integrating `tap-google-analytics` with another system, like Stitch Data might do to allow users to configure their extracts themselves without manual config setup. This tap expects an `access_token`, `refresh_token`, `client_id` and `client_secret` to be passed to it in order to authenticate as the user who granted the token and then access their data.

## Required Analytics Reporting APIs & OAuth Scopes

In order for `tap-google-analytics` to access your Google Analytics Account, it needs the Analytics Reporting API *and* the Analytics API (which are two different things) enabled. If using a service account to authorize, these need to be enabled for a project inside the same organization as your Google Analytics account (see below), or if using an OAuth credential set, they need to be enabled for the project the OAuth client ID and secret come from.

If using the OAuth authorization method, the OAuth flow conducted elsewhere must request at minimum the `analytics.readonly` OAuth scope to get an `access_token` authorized to hit these APIs

### Creating service account credentials

If you have already have a valid `client_secrets.json` for a service account, or if you are using OAuth based authorization, you can skip the rest of this section.

As a first step, you need to create or use an existing project in the Google Developers Console:

1. Sign in to the Google Account you are using for managing Google Analytics (you must have Manage Users permission at the account, property, or view level).

2. Open the [Service accounts page](https://console.developers.google.com/iam-admin/serviceaccounts). If prompted, select a project or create a new one to use for accessing Google Analytics.

3. Click Create service account.

   In the Create service account window, type a name for the service account, and select Furnish a new private key. Then click Save and store it locally as `client_secrets.json`.

   If you already have a service account, you can generate a key by selecting 'Edit' for the account and then selecting the option to generate a key.

Your new public/private key pair is generated and downloaded to your machine; it serves as the only copy of this key. You are responsible for storing it securely.

### Add service account to the Google Analytics account

The newly created service account will have an email address that looks similar to:

```
quickstart@PROJECT-ID.iam.gserviceaccount.com
```

Use this email address to [add a user](https://support.google.com/analytics/answer/1009702) to the Google analytics view you want to access via the API. For using `tap-google-analytics` only [Read & Analyze permissions](https://support.google.com/analytics/answer/2884495) are needed.

### Enable the APIs

1. Visit the [Google Analytics Reporting API](https://console.developers.google.com/apis/api/analyticsreporting.googleapis.com/overview) dashboard and make sure that the project you used in the `Create credentials` step is selected.

From this dashboard, you can enable/disable the API for your account, set Quotas and check usage stats for the service account you are using with `tap-google-analytics`.

2. Visit the [Google Analytics API](https://console.developers.google.com/apis/api/analytics.googleapis.com/overview) dashboard, make sure that the project you used in the `Create credentials` step is selected and enable the API for your account.



---

## Tap Configuration

A sample config for `tap-google-analytics` might look like this.

```json
{
  "key_file_location": "service_account_file.json",
  "view_id": "1234566",
  "reports": "reports.json",
  "start_date": "2018-01-01T00:00:00Z",
  "end_date": "2019-01-01T00:00:00Z",
  "sampling_level": "DEFAULT",
  "segment_id": "gaid::xxxxx",
  "lookback_days": 10,
  "date_batching": "WEEK"
}
```

### Required Fields

- `view_id`: GA view ID
- `start_date`: Timestamp for the report start date, formatted yyyy-mm-ddThh:mm
- `key_file_location`: Path for the Google Cloud project service account.

- `oauth_credentials`: If using OAuth based authorization, a nested JSON object with the whole config looking like this:

```json
{
  "oauth_credentials": {
      "access_token": "<ya29.GlxtB_access_token_gobbledegook>",
      "refresh_token": "<ya29.GlxtB_refresh_tokeN_gobbledegook>",
      "client_id": "<something.apps.googleusercontent.com>",
      "client_secret": "<some client secret string>"
  },
  "view_id": ...
}
```


### Optional Fields

- `reports`: Path for the local JSON file which contains report definitions. If omitted, it will use the default definitions located at _/defaults/default_report_definitions.json_
- `end_date`: The end date for the report, formatted yyyy-mm-ddThh:mm. If omitted, it will default to yesterday.
- `sampling_level`: Sampling level to be used for GA API queries. Can be DEFAULT, SMALL or LARGE. If omitted, it will default to `DEFAULT`.
- `segment_id`: Segment ID for the specific segment you'd like to query data.
- `lookback_days`: Number of days prior to the report state date the tap should look back. If omitted, it will default to 15.
- `date_batching`: How the report date range should be batched to run API queries on smaller chunks. Can be `DAY`, `WEEK` or `MONTH`.

---
## Stream Definitions

Stream definitions need to be created within a single JSON file, in an array. Each report definition needs to have:
- `name:` A unique name to identify the stream.
- `dimensions:` An array of GA dimensions to be included in the stream.
- `metrics`: An array of GA metrics to be included in the stream.

In order to run these streams, you will need to include the filename within the tap's config file, in the `reports` field.

Here's what an example `reports.json` file looks like:

```json
[
  { "name" : "users_per_day",
    "dimensions" :
    [
      "ga:date"
    ],
    "metrics" :
    [
      "ga:users",
      "ga:newUsers"
    ]
  },
  { "name" : "channel_report",
    "dimensions" :
    [
      "ga:date",
      "ga:channelGruoping",
      "ga:sourceMedium"
    ],
    "metrics" :
    [
      "ga:sessions",
      "ga:sessionsPerUser",
      "ga:avgSessionDuration",
      "ga:transactions",
      "ga:transactionRevenue"
    ]
  }
]
```
---
## Running the Tap

By default, the tap can be run with this command:

```
tap-google-analytics --config config.json
```

If you'd like to activate incremental updates using a `state.json` file, please follow these steps:

- Create an empty `state.json` file at the project root, with an empty object inside.
- Run the tap as usual, but append `--state state.json` to the tap activation command.
- Add `>> state.json` at the end of the tap activation command to make sure that the state is updated at the end of each run.

A full pipeline utilising the state functionality for this tap would look like this:

```
tap-google-analytics --config config.json --state state.json | target-xxx --config target-config.json >> state.json
```

## Implementation Notes

The following decisions and considerations have been done while building the tap:

- By default, all the reports configured within the report definition file is included while syncing streams. If you'd like to stop replicating a specific report, you can just remote if from the report definitions file.
- All the metric and dimension names are changed in the output, such as `ga:date > ga_date`.
- Two additional properties, `report_start_date` and `report_end_date` are added in the tap schema and outputs.
- An additional `_sdc_record_hash` property is added in the schema and output. This is a hash value for the array `[view_id, list of dimensions]` and is also defined as a key property in the stream schema. It could be used in your target database to update existing values.
- An additional `_sdc_record_timestamp` property is added in the schema and output. This is the timestamp of the Google Analytics API.
