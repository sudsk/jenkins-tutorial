#!/usr/bin/env python
# Parses the command line and starts the script

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import textwrap

import argparse

from bq_dataset_mover import dataset_mover_service

def _parse_yaml_file(path):
    """Load and parse local YAML file

    Args:
        path: a local file system path

    Returns:
        a Python object representing the parsed YAML data
    """

    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as ex:
            print(ex)


def main():
    """Get passed in args and run either a test run or an actual move"""
    parsed_args = _get_parsed_args()

    # Load the config values set in the config file and create the storage clients.
    config = configuration.Configuration.from_conf(parsed_args)

    # Create the cloud logging client that will be passed to all other modules.
    cloud_logger = config.target_logging_client.logger('bq-dataset-mover')  # pylint: disable=no-member
    
    if parsed_args.test:
        test_bucket_name = bucket_mover_tester.set_up_test_bucket(
            config, parsed_args)
        config.bucket_name = test_bucket_name
        config.target_bucket_name = test_bucket_name

    bq_mover_service.main(config, parsed_args, cloud_logger)
