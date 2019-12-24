#!/usr/bin/env python3
import sys
import json
from datetime import date, timedelta

import singer
from singer import utils, metadata, get_bookmark, write_bookmark, write_state

from tap_google_analytics.client import GAClient
from tap_google_analytics.reports_helper import ReportsHelper

from pathlib import Path

LOGGER = singer.get_logger()

def generate_sdc_record_hash(raw_report, row, start_date, end_date):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a UTF-8 encoded JSON list
    containing:
    - The account_id, web_property_id, profile_id of the associated report
    - Pairs of ("ga:dimension_name", "dimension_value")
    - Report start_date value in YYYY-mm-dd format
    - Report end_date value in YYYY-mm-dd format
    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.
    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """
    dimensions_headers = raw_report["reports"][0]["columnHeader"]["dimensions"]
    profile_id = raw_report["profileId"]
    web_property_id = raw_report["webPropertyId"]
    account_id = raw_report["accountId"]

    dimensions_pairs = sorted(zip(dimensions_headers, row["dimensions"]), key=lambda x: x[0])

    # NB: Do not change the ordering of this list, it is the source of the PK hash
    hash_source_data = [account_id,
                        web_property_id,
                        profile_id,
                        dimensions_pairs,
                        start_date.strftime("%Y-%m-%d"),
                        end_date.strftime("%Y-%m-%d")]

    hash_source_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_source_bytes).hexdigest()

def get_start_date(config, state, stream_name):
    """
    Returns a date bookmark in state for the given stream, or the
    `start_date` from config, if no bookmark exists.
    """
    return utils.strptime_to_utc(get_bookmark(state, stream_name, 'last_report_date', default=config['start_date'].strftime('%Y-%m-%d')))

def get_end_date(config):
    """
    Returns the end_date for the reporting sync. Under normal operation,
    this is defined as the last full day to occur before UTC now.
    This can be overridden by the `end_date` config.json value.
    """
    if 'end_date' in config: return config['end_date']
    return (utils.now() - timedelta(1)).replace(hour=0, minute=0, second=0, microsecond=0)

def discover(config):
    # Load the reports json file
    default_reports = Path(__file__).parent.joinpath('defaults', 'default_report_definition.json')

    report_def_file = config.get('reports', default_reports)
    if Path(report_def_file).is_file():
        try:
            reports_definition = load_json(report_def_file)
        except ValueError:
            LOGGER.critical("tap-google-analytics: The JSON definition in '{}' has errors".format(report_def_file))
            sys.exit(1)
    else:
        LOGGER.critical("tap-google-analytics: '{}' file not found".format(report_def_file))
        sys.exit(1)

    # validate the definition
    reports_helper = ReportsHelper(config, reports_definition)
    reports_helper.validate()

    # Generate and return the catalog
    return reports_helper.generate_catalog()

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks for an empty breadcrumb
    and metadata with a 'selected' or an 'inclusion' == automatic entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = metadata.to_map(stream['metadata'])

        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected") \
          or metadata.get(stream_metadata, (), "inclusion") == 'automatic':
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def sync(config, state, catalog):
    errors_encountered = False

    selected_stream_ids = get_selected_streams(catalog)

    client = GAClient(config)

    if not state.get('bookmarks'):
        state['bookmarks'] = {}

    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        report_definition = ReportsHelper.get_report_definition(stream)

        stream_metadata = metadata.to_map(stream['metadata'])
        key_properties = metadata.get(stream_metadata, (), "table-key-properties")

        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: ' + stream_id)

            start_date = get_start_date(config, state, stream_id)
            end_date = get_end_date(config)
            date_range = []

            while start_date <= end_date:
                date_range.append(utils.strftime(start_date ,'%Y-%m-%d'))
                start_date += timedelta(days=1)

            for date in date_range:
                try:
                    results = client.process_stream(date, report_definition)

                    # we write the schema message after we are sure that we could
                    #  fetch records without errors
                    singer.write_schema(stream_id, stream_schema, key_properties)
                    singer.write_records(stream_id, results)
                    singer.write_bookmark(state, stream_id, 'last_report_date', date)
                    singer.write_state(state)
                except TapGaInvalidArgumentError as e:
                    errors_encountered = True
                    LOGGER.error("Skipping stream: '{}' due to invalid report definition.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                except TapGaRateLimitError as e:
                    errors_encountered = True
                    LOGGER.error("Skipping stream: '{}' due to Rate Limit Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                except TapGaQuotaExceededError as e:
                    errors_encountered = True
                    LOGGER.error("Skipping stream: '{}' due to Quota Exceeded Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                except TapGaAuthenticationError as e:
                    LOGGER.error("Stopping execution while processing '{}' due to Authentication Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                    sys.exit(1)
                except TapGaUnknownError as e:
                    LOGGER.error("Stopping execution while processing '{}' due to Unknown Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                    sys.exit(1)
        else:
            LOGGER.info('Skipping unselected stream: ' + stream_id)

    # If we encountered errors, exit with 1
    if errors_encountered:
        sys.exit(1)

    return