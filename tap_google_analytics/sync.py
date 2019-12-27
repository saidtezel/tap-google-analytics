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

def batch_report_dates(start_date, end_date, interval):
    """
    Generate tuples with intervals from a given range of dates.

    batch_report_dates('2018-01-01', '2019-12-25', 6)

    1st yield = ('2018-01-01', '2018-01-07')
    2nd yield = ('2018-01-08', '2018-01-14')
    """
    date_diff = (end_date - start_date).days

    # If the date range is smaller than 30 days, opt for daily batching.
    if date_diff < 30:
        interval = 0

    span = timedelta(days=interval)
    stop_date = end_date - span

    while start_date < stop_date:
        current_date = start_date + span
        yield start_date, current_date
        start_date = current_date + timedelta(days=1)

    yield start_date, end_date


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
            date_interval = config['date_batching']

            LOGGER.info(f'Syncing stream: {stream_id}')
            LOGGER.info(f'Will sync data from {start_date.isoformat()} until {end_date.isoformat()}')

            # Sets the currently sycing stream in state
            singer.set_currently_syncing(state, stream_id)
            # Writes the schema for the current stream
            singer.write_schema(stream_id, stream_schema, key_properties)

            for start_date, end_date in batch_report_dates(start_date, end_date, date_interval):
                LOGGER.info(f'Request for {start_date.isoformat()} to {end_date.isoformat()} started.')
                start = timer()
                try:
                    results = client.process_stream(start_date, end_date, report_definition)

                    # Writes individual items from results array as records
                    singer.write_records(stream_id, results)
                    # Updates the stream bookmark with the latest report timestamp
                    singer.write_bookmark(state, stream_id, 'last_report_date', end_date.strftime("%Y-%m-%d"))
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
                LOGGER.info(f'Request for {start_date.isoformat()} to {end_date.isoformat()} finished.')
                LOGGER.info(f'API query took {(end-start):.2f} seconds.')

            singer.set_currently_syncing(state, '')
            singer.write_state(state)
        else:
            LOGGER.info('Skipping unselected stream: ' + stream_id)

    # If we encountered errors, exit with 1
    if errors_encountered:
        sys.exit(1)

    return