import json
import sys
from pathlib import Path

import singer
from singer import metadata
from singer.catalog import Catalog
from singer.schema import Schema

from .helpers import *
from .client import Client


LOGGER = singer.get_logger()

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
    report = Report(config, reports_definition)
    report.validate()

    # Generate and return the catalog
    return report.generate_catalog()

class Report:
    def __init__(self, config, reports_definition):
        self.reports_definition = reports_definition
        # Fetch the valid (dimension, metric) names and their types from GAClient
        self.client = Client(config)

    def generate_catalog(self):
        catalog = {
            "streams": []
        }

        for report in self.reports_definition:
            stream_name = report['name']
            table_key_properties = ['_sdc_record_hash']
            replication_key = '_sdc_record_timestamp'
            schema = {
                "type": ["null", "object"],
                "additionalProperties": False,
                "properties": {
                    "_sdc_record_hash": {
                        "type": ['string']
                    },
                    "_sdc_record_timestamp": {
                        "type": ["string"],
                        "format": "date-time"
                    },
                    "report_start_date": {
                        "type": ["string"],
                        "format": "date-time"
                    },
                    "report_end_date": {
                        "type": ["string"],
                        "format": "date-time"
                    }
                }
            }
            metadata = []

            for dimension in report['dimensions']:
                data_type = self.client.lookup_data_type('dimension', dimension)
                dimension = dimension.replace("ga:","ga_")
                schema['properties'][dimension] = {
                    "type": [data_type],
                }
                table_key_properties.append(dimension)

                metadata.append({
                    "metadata": {
                        "inclusion": "automatic",
                        "selected-by-default": True,
                        "ga_type": 'dimension'
                    },
                    "breadcrumb": ["properties", dimension]
                })

            for metric in report['metrics']:
                data_type = self.client.lookup_data_type('metric', metric)
                metric = metric.replace("ga:","ga_")

                schema['properties'][metric] = {
                    "type": ["null", data_type],
                }

                metadata.append({
                    "metadata": {
                        "inclusion": "automatic",
                        "selected-by-default": True,
                        "ga_type": 'metric'
                    },
                    "breadcrumb": ["properties", metric]
                })

            stream_metadata = {
                "metadata": {
                    "inclusion": "automatic",
                    "table-key-properties": table_key_properties,
                    "replication-method": "INCREMENTAL",
                    "replication-key": replication_key,
                    "schema-name": stream_name
                },
                "breadcrumb": []
            }

            metadata.insert(0, stream_metadata)

            catalog_entry = {
                "stream_name": stream_name,
                "tap_stream_id": stream_name,
                "schema": schema,
                "metadata": metadata
            }
            catalog['streams'].append(catalog_entry)

        return catalog

    def validate(self):
        for report in self.reports_definition:
            try:
                name = report['name']
                dimensions = report['dimensions']
                metrics = report['metrics']
            except KeyError:
                LOGGER.critical("Report definition is missing one of the required properties (name, dimensions, metrics)")
                sys.exit(1)

            # Check that not too many metrics && dimensions have been requested
            if len(metrics) == 0:
                LOGGER.critical("'{}' has no metrics defined. GA reports must specify at least one metric.".format(name))
                sys.exit(1)
            elif len(metrics) > 10:
                LOGGER.critical("'{}' has too many metrics defined. GA reports can have maximum 10 metrics.".format(name))
                sys.exit(1)

            if len(dimensions) > 7:
                LOGGER.critical("'{}' has too many dimensions defined. GA reports can have maximum 7 dimensions.".format(name))
                sys.exit(1)

            self.validate_dimensions(dimensions)
            self.validate_metrics(metrics)

    def validate_dimensions(self, dimensions):
        # check that all the dimensions are proper Google Analytics Dimensions
        for dimension in dimensions:
            if not dimension.startswith(('ga:dimension', 'ga:customVarName', 'ga:customVarValue', 'ga:segment')) \
               and dimension not in self.client.dimensions_ref:
                LOGGER.critical("'{}' is not a valid Google Analytics dimension".format(dimension))
                LOGGER.info("For details see https://developers.google.com/analytics/devguides/reporting/core/dimsmets")
                sys.exit(1)

    def validate_metrics(self, metrics):
        # check that all the metrics are proper Google Analytics metrics
        for metric in metrics:
            if metric.startswith('ga:goal') and metric.endswith(('Starts', 'Completions', 'Value', 'ConversionRate', 'Abandons', 'AbandonRate')):
                # Custom Google Analytics Metrics {ga:goalXXStarts, ga:goalXXValue, ... }
                continue
            elif metric.startswith('ga:searchGoal') and metric.endswith('ConversionRate'):
                # Custom Google Analytics Metrics ga:searchGoalXXConversionRate
                continue
            elif not metric.startswith(('ga:metric', 'ga:calcMetric')) \
               and metric not in self.client.metrics_ref:
                LOGGER.critical("'{}' is not a valid Google Analytics metric".format(metric))
                LOGGER.info("For details see https://developers.google.com/analytics/devguides/reporting/core/dimsmets")
                sys.exit(1)

    @staticmethod
    def get_report_definition(stream):
        report = {
            "name" : stream['tap_stream_id'],
            "dimensions" : [],
            "metrics" : []
        }

        stream_metadata = singer.metadata.to_map(stream['metadata'])

        for attribute in stream['schema']['properties'].keys():
            ga_type = singer.metadata.get(stream_metadata, ('properties', attribute), "ga_type")

            if ga_type == 'dimension':
                report['dimensions'].append(attribute)
            elif ga_type == 'metric':
                report['metrics'].append(attribute)

        return report