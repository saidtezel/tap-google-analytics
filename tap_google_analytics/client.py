import sys
import backoff
import logging
import json
import singer
import socket
import hashlib
from datetime import datetime

from google.oauth2 import service_account
import googleapiclient.discovery

from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials

from .error import *
from .helpers import generate_sdc_record_hash

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

NON_FATAL_ERRORS = [
  'userRateLimitExceeded',
  'rateLimitExceeded',
  'quotaExceeded',
  'internalServerError',
  'backendError'
]

# Silence the Analytics API info messages
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
LOGGER = singer.get_logger()


def error_reason(e):
    # For a given HttpError object from the googleapiclient package, this returns the first reason code from
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors if the error's HTTP response
    # body is valid json. Note that the code samples for Python on that page are actually incorrect, and that
    # e.resp.reason is the HTTP transport level reason associated with the status code, like "Too Many Requests"
    # for a 429 response code, whereas we want the reason field of the first error in the JSON response body.

    reason = ''
    try:
        data = json.loads(e.content.decode('utf-8'))
        reason = data['error']['errors'][0]['reason']
    except Exception:
        pass

    return reason


def is_fatal_error(error):
    if isinstance(error, socket.timeout):
        return False

    status = error.resp.status if getattr(error, 'resp') is not None else None
    if status in [500, 503]:
        return False

    # Use list of errors defined in:
    # https://developers.google.com/analytics/devguides/reporting/core/v4/errors
    reason = error_reason(error)
    if reason in NON_FATAL_ERRORS:
        return False

    LOGGER.critical("Received fatal error %s, reason=%s, status=%s", error, reason, status)
    return True

class Client:
    def __init__(self, config):
        self.view_id = config.get('view_id')
        self.quota_user = config.get('quota_user', None)
        self.sampling_level = config.get('sampling_level', 'DEFAULT')
        self.credentials = self.initialize_credentials(config)
        self.analytics = self.initialize_analyticsreporting()

        (self.dimensions_ref, self.metrics_ref) = self.fetch_metadata()

    def initialize_credentials(self, config):
        if 'oauth_credentials' in config:
            return GoogleCredentials(
                access_token=config['oauth_credentials']['access_token'],
                refresh_token=config['oauth_credentials']['refresh_token'],
                client_id=config['oauth_credentials']['client_id'],
                client_secret=config['oauth_credentials']['client_secret'],
                token_expiry=None,  # let the library refresh the token if it is expired
                token_uri="https://accounts.google.com/o/oauth2/token",
                user_agent="tap-google-analytics (via singer.io)"
            )
        else:
            return service_account.Credentials.from_service_account_file(
                config['key_file_location'],
                scopes=SCOPES
            )
            # return ServiceAccountCredentials.from_json_keyfile_dict(config['client_secrets'], SCOPES)

    def initialize_analyticsreporting(self):
        """Initializes an Analytics Reporting API V4 service object.

        Returns:
            An authorized Analytics Reporting API V4 service object.
        """
        return googleapiclient.discovery.build('analyticsreporting', 'v4', credentials=self.credentials)
        # return build('analyticsreporting', 'v4', credentials=self.credentials)

    def fetch_metadata(self):
        """
        Fetch the valid (dimensions, metrics) for the Analytics Reporting API
         and their data types.

        Returns:
          A map of (dimensions, metrics) hashes

          Each available dimension can be found in dimensions with its data type
            as the value. e.g. dimensions['ga:userType'] == STRING

          Each available metric can be found in metrics with its data type
            as the value. e.g. metrics['ga:sessions'] == INTEGER
        """
        metrics = {}
        dimensions = {}

        # Initialize a Google Analytics API V3 service object and build the service object.
        # This is needed in order to dynamically fetch the metadata for available
        #   metrics and dimensions.
        # (those are not provided in the Analytics Reporting API V4)
        service = build('analytics', 'v3', credentials=self.credentials)

        results = service.metadata().columns().list(reportType='ga', quotaUser=self.quota_user).execute()

        columns = results.get('items', [])

        for column in columns:
            column_attributes = column.get('attributes', [])

            column_name = column.get('id')
            column_type = column_attributes.get('type')
            column_data_type = column_attributes.get('dataType')

            if column_type == 'METRIC':
                metrics[column_name] = column_data_type
            elif column_type == 'DIMENSION':
                dimensions[column_name] = column_data_type

        return (dimensions, metrics)

    def lookup_data_type(self, type, attribute):
        """
        Get the data type of a metric or a dimension
        """
        try:
            if type == 'dimension':
                # Custom GA dimensions that are not part of self.dimensions_ref
                # They are almost always strings
                if attribute.startswith(('ga:dimension', 'ga:customVarName', 'ga:customVarValue', 'ga:segment')):
                    return 'string'

                attr_type = self.dimensions_ref[attribute]
            elif type == 'metric':
                # Custom GA metrics that are not part of self.metrics_ref
                # They can be integer of number but we'll assume they're numbers just to be on the safe side.
                if attribute.startswith('ga:goal') and attribute.endswith(('Starts', 'Completions', 'Value', 'ConversionRate', 'Abandons', 'AbandonRate')):
                    return 'number'
                elif attribute.startswith('ga:searchGoal') and attribute.endswith('ConversionRate'):
                    return 'number'
                elif attribute.startswith(('ga:metric', 'ga:calcMetric')):
                    return 'number'

                attr_type = self.metrics_ref[attribute]
            else:
                LOGGER.critical(f"Unsupported GA type: {type}")
                sys.exit(1)
        except KeyError:
            LOGGER.critical(f"Unsupported GA {type}: {attribute}")
            sys.exit(1)

        data_type = 'string'

        if attr_type == 'INTEGER':
            data_type = 'integer'
        elif attr_type == 'FLOAT' or attr_type == 'PERCENT' or attr_type == 'TIME' or attr_type == 'CURRENCY':
            data_type = 'number'

        return data_type

    def process_stream(self, start_date, end_date, stream, segment_id):
        LOGGER.info(f'process_stream segment_id: {segment_id}')
        try:
            records = []
            report_definition = self.generate_report_definition(stream)
            nextPageToken = None

            while True:
                single_response = self.query_api(start_date, end_date, report_definition, nextPageToken, segment_id)
                (nextPageToken, results) = self.process_response(start_date, end_date, single_response)
                records.extend(results)

                # Keep on looping as long as we have a nextPageToken
                if nextPageToken is None:
                    break

            return records
        except HttpError as e:
            # Process API errors
            # Use list of errors defined in:
            # https://developers.google.com/analytics/devguides/reporting/core/v4/errors

            reason = error_reason(e)
            if reason == 'userRateLimitExceeded' or reason == 'rateLimitExceeded':
                raise GaRateLimitError(e._get_reason())
            elif reason == 'quotaExceeded':
                raise GaQuotaExceededError(e._get_reason())
            elif e.resp.status == 400:
                raise GaInvalidArgumentError(e._get_reason())
            elif e.resp.status in [401, 402]:
                raise GaAuthenticationError(e._get_reason())
            elif e.resp.status in [500, 503]:
                raise GaBackendServerError(e._get_reason())
            else:
                raise GaUnknownError(e._get_reason())

    def generate_report_definition(self, stream):
        report_definition = {
            'metrics': [],
            'dimensions': []
        }

        for dimension in stream['dimensions']:
            report_definition['dimensions'].append({'name': dimension.replace("ga_","ga:")})

        for metric in stream['metrics']:
            report_definition['metrics'].append({"expression": metric.replace("ga_","ga:")})

        return report_definition

    @backoff.on_exception(backoff.expo,
                          (HttpError, socket.timeout),
                          max_tries=10,
                          giveup=is_fatal_error)
    def query_api(self, start_date, end_date, report_definition, pageToken=None, segment_id=None):
        """Queries the Analytics Reporting API V4.

        Returns:
            The Analytics Reporting API V4 response.
        """
        request_body = {
            'reportRequests': [
            {
                'viewId': self.view_id,
                'dateRanges': [{'startDate': start_date.strftime("%Y-%m-%d"), 'endDate': end_date.strftime("%Y-%m-%d")}],
                'samplingLevel': self.sampling_level,
                'pageSize': '100000',
                'pageToken': pageToken,
                'metrics': report_definition['metrics'],
                'dimensions': report_definition['dimensions']
            }]
        }
        if segment_id:
            request_body['reportRequests'][0]['segments'] = [{
                'segmentId': segment_id
            }]
        return self.analytics.reports().batchGet(
            body=request_body,
            quotaUser=self.quota_user
        ).execute()

    def process_response(self, start_date, end_date, response):
        """Processes the Analytics Reporting API V4 response.

        Args:
            response: An Analytics Reporting API V4 response.

        Returns: (nextPageToken, results)
            nextPageToken: The next Page Token
             If it is not None then the maximum pageSize has been reached
             and a followup call must be made using self.query_api().
            results: the Analytics Reporting API V4 response as a list of
             dictionaries, e.g.
             [
              {'ga_date': '20190501', 'ga_30dayUsers': '134420',
               'report_start_date': '2019-05-01', 'report_end_date': '2019-05-28'},
               ... ... ...
             ]
        """
        start_date_string = start_date.isoformat()
        end_date_string = end_date.isoformat()
        results = []

        try:
            # We always request one report at a time
            report = next(iter(response.get('reports', [])), None)

            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

            for row in report.get('data', {}).get('rows', []):
                record = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                for header, dimension in zip(dimensionHeaders, dimensions):
                    data_type = self.lookup_data_type('dimension', header)

                    if data_type == 'integer':
                        value = int(dimension)
                    elif data_type == 'number':
                        value = float(dimension)
                    else:
                        value = dimension

                    record[header.replace("ga:","ga_")] = value

                for i, values in enumerate(dateRangeValues):
                    for metricHeader, value in zip(metricHeaders, values.get('values')):
                        metric_name = metricHeader.get('name')
                        metric_type = self.lookup_data_type('metric', metric_name)

                        if metric_type == 'integer':
                            value = int(value)
                        elif metric_type == 'number':
                            value = float(value)

                        record[metric_name.replace("ga:","ga_")] = value

                # Also add the [start_date,end_date] used for the report
                record['report_start_date'] = start_date_string
                record['report_end_date'] = end_date_string
                
                # If there is no date within requested dimensions, append the report_start_date to the dimensionHeaders
                # to make sure that the record hash includes a unique report timestamp
                if 'ga:date' not in dimensionHeaders:
                    dimensions.append(start_date_string)

                record['_sdc_record_hash'] = generate_sdc_record_hash(self.view_id, dimensions)
                record['_sdc_record_timestamp'] = datetime.now().isoformat()

                results.append(record)

            return (report.get('nextPageToken'), results)
        except StopIteration:
            return (None, [])
