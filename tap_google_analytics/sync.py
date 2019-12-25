import json
import hashlib
import sys
from datetime import timedelta
from timeit import default_timer as timer

import singer
from singer import utils, metadata, get_bookmark

from .client import Client
from .discover import Report
from .error import *

LOGGER = singer.get_logger()

def generate_report_dates(start_date, end_date):
    total_days = (end_date - start_date).days
    # NB: Add a day to be inclusive of both start and end
    for day_offset in range(total_days + 1):
        yield start_date + timedelta(days=day_offset)

def generate_report_dates_new(start_date, end_date):
    offset = 7
    total_days = (end_date - start_date).days
    total_days_range = list(range(total_days))
    for day_offset in total_days_range[1::offset]:
        yield (start_date + timedelta(days=day_offset-offset+1), start_date + timedelta(days=day_offset))
        # yield start_date + timedelta(days=day_offset)
    if total_days > total_days_range[-1]:
        diff = total_days - total_days_range[-1]
        yield (start_date + timedelta(days=total_days-diff), start_date + timedelta(days=total_days))


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

    client = Client(config)

    # Check if there are existing bookmarks, if not create a new one
    state['bookmarks'] = state.get('bookmarks', {})

    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        report_definition = Report.get_report_definition(stream)

        stream_metadata = metadata.to_map(stream['metadata'])
        key_properties = metadata.get(stream_metadata, (), "table-key-properties")

        if stream_id in selected_stream_ids:
            start_date = utils.strptime_to_utc(get_bookmark(state, stream_id, 'last_report_date', default=config['start_date'].strftime('%Y-%m-%d')))
            start_date = start_date - timedelta(days=config.get('lookback_days', 15))
            end_date = config['end_date']

            singer.set_currently_syncing(state, stream_id)

            LOGGER.info(f'Syncing stream: {stream_id}')
            LOGGER.info(f'Will sync data from {start_date.isoformat()} until {end_date.isoformat()}')

            for date in generate_report_dates(start_date, end_date):
                LOGGER.info(f'Request for {date.isoformat()} started.')
                start = timer()
                try:
                    results = client.process_stream(date, report_definition)

                    # we write the schema message after we are sure that we could
                    #  fetch records without errors
                    singer.write_schema(stream_id, stream_schema, key_properties)
                    singer.write_records(stream_id, results)
                    singer.write_bookmark(state, stream_id, 'last_report_date', date.strftime("%Y-%m-%d"))
                    singer.write_state(state)
                except GaInvalidArgumentError as e:
                    errors_encountered = True
                    LOGGER.error("Skipping stream: '{}' due to invalid report definition.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                except GaRateLimitError as e:
                    errors_encountered = True
                    LOGGER.error("Skipping stream: '{}' due to Rate Limit Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                except GaQuotaExceededError as e:
                    errors_encountered = True
                    LOGGER.error("Skipping stream: '{}' due to Quota Exceeded Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                except GaAuthenticationError as e:
                    LOGGER.error("Stopping execution while processing '{}' due to Authentication Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                    sys.exit(1)
                except GaUnknownError as e:
                    LOGGER.error("Stopping execution while processing '{}' due to Unknown Errors.".format(stream_id))
                    LOGGER.debug("Error: '{}'.".format(e))
                    sys.exit(1)
                end = timer()
                LOGGER.info(f'Request for {date.isoformat()} finished.\nIt took {end-start} seconds.')

            singer.set_currently_syncing(state, '')
            singer.write_state(state)
        else:
            LOGGER.info('Skipping unselected stream: ' + stream_id)

    # If we encountered errors, exit with 1
    if errors_encountered:
        sys.exit(1)

    return