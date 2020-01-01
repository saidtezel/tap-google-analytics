#!/usr/bin/env python3
from datetime import timedelta, date
import json
import sys
import re

from pathlib import Path

import singer
from singer import utils, get_bookmark

from .sync import sync
from .discover import discover
from .helpers import *
from .error import *

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "view_id"
]

LOGGER = singer.get_logger()

def get_start_date(config, state, stream_name):
    """
    Returns a date bookmark in state for the given stream, or the
    `start_date` from config, if no bookmark exists.
    """
    return utils.strptime_to_utc(get_bookmark(state, stream_name, 'last_report_date', default=config['start_date']))

def get_end_date(config):
    """
    Returns the end_date for the reporting sync. Under normal operation,
    this is defined as the last full day to occur before UTC now.
    This can be overridden by the `end_date` config.json value.
    """
    if 'end_date' in config: return config['end_date']
    return (utils.now() - timedelta(1)).replace(hour=0, minute=0, second=0, microsecond=0)

def process_args():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Check for errors on the provided config params that utils.parse_args is letting through
    if not args.config.get('start_date'):
        LOGGER.critical("tap-google-analytics: a valid start_date must be provided.")
        sys.exit(1)

    if not args.config.get('view_id'):
        LOGGER.critical("tap-google-analytics: a valid view_id must be provided.")
        sys.exit(1)

    if not args.config.get('key_file_location') and not args.config.get('oauth_credentials'):
        LOGGER.critical("tap-google-analytics: a valid key_file_location string or oauth_credentials object must be provided.")
        sys.exit(1)

    # Remove optional args that have empty strings as values
    # Check if sampling level is defined and valid.
    if 'sampling_level' in args.config and args.config.get('sampling_level') not in ['DEFAULT', 'SMALL', 'LARGE']:
        LOGGER.warning('tap-google-analytics: Invalid sampling_level, will default to DEFAULT')
        del args.config['sampling_level']

    # Check if lookback days is defined and valid.
    if 'lookback_days' in args.config and type(args.config.get('lookback_days')) is not int:
        LOGGER.warning('tap-google-analytics: Invalid lookback_days, will default to 15')
        del args.config['lookback_days']

    if 'reports' in args.config and not args.config.get('reports'):
        del args.config['reports']

    if 'end_date' in args.config and not args.config.get('end_date'):
        del args.config['end_date']

    if 'date_batching' in args.config and not args.config.get('date_batching') in ['DAY', 'WEEK', 'MONTH']:
        del args.config['date_batching']

    # Process the start_date and end_date so that they define an open date window
    # that ends yesterday if end_date is not defined
    start_date = utils.strptime_to_utc(args.config['start_date'])
    args.config['start_date'] = start_date

    end_date = args.config.get('end_date', utils.strftime(utils.now()))
    end_date = utils.strptime_to_utc(end_date)
    # end_date = utils.strptime_to_utc(end_date) - timedelta(days=1)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    args.config['end_date'] = end_date

    if end_date < start_date:
        LOGGER.critical("tap-google-analytics: start_date '{}' > end_date '{}'".format(start_date, end_date))
        sys.exit(1)

    date_batching = args.config.get('date_batching', 'DAY')
    if date_batching == 'DAY':
        args.config['date_batching'] = 0
    elif date_batching == 'WEEK':
        args.config['date_batching'] = 6
    elif date_batching == 'MONTH':
        args.config['date_batching'] = 29

    # If using a service account, validate that the client_secrets.json file exists and load it
    if args.config.get('key_file_location'):
        if Path(args.config['key_file_location']).is_file():
            try:
                args.config['client_secrets'] = load_json(args.config['key_file_location'])
            except ValueError:
                LOGGER.critical("tap-google-analytics: The JSON definition in '{}' has errors".format(args.config['key_file_location']))
                sys.exit(1)
        else:
            LOGGER.critical("tap-google-analytics: '{}' file not found".format(args.config['key_file_location']))
            sys.exit(1)
    else:
        # If using oauth credentials, verify that all required keys are present
        credentials = args.config['oauth_credentials']

        if not credentials.get('access_token'):
            LOGGER.critical("tap-google-analytics: a valid access_token for the oauth_credentials must be provided.")
            sys.exit(1)

        if not credentials.get('refresh_token'):
            LOGGER.critical("tap-google-analytics: a valid refresh_token for the oauth_credentials must be provided.")
            sys.exit(1)

        if not credentials.get('client_id'):
            LOGGER.critical("tap-google-analytics: a valid client_id for the oauth_credentials must be provided.")
            sys.exit(1)

        if not credentials.get('client_secret'):
            LOGGER.critical("tap-google-analytics: a valid client_secret for the oauth_credentials must be provided.")
            sys.exit(1)

    return args

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = process_args()

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(args.config)
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog.to_dict()
        else:
            catalog = discover(args.config)

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
