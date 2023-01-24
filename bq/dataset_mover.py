#!/usr/bin/env python
"""Script to move a bucket, all settings and data from one project to another."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime
import json
from time import sleep, time
from retrying import retry
from yaspin import yaspin
import argparse

from google.api_core import iam as api_core_iam
from google.cloud import exceptions
from google.cloud import bigquery
from google.cloud import logging
from google.cloud import bigquery_datatransfer_v1
from googleapiclient import discovery
from google.oauth2 import service_account
from google.protobuf.timestamp_pb2 import Timestamp

def main():
    """Main entry point for the dataset mover tool

    Args:
        project_name: GCP Project Name
        dataset_name: BigQuery Dataset Name which have to be moved
        service_account_key: Path and Name of the service account key json file
    """
    """Get passed in args and run either a test run or an actual move"""
    args = _get_parsed_args()
    project_id = args.project_id
    dataset_name = args.dataset_name
    service_account_key = args.service_account_key
    
    sa_credentials = service_account.Credentials.from_service_account_file(service_account_key)
    sa_email = sa_credentials.service_account_email
    
    # Create the cloud logging client that will be passed to all other modules.
    logging_client = logging.Client(credentials=sa_credentials, project = project_id)
    cloud_logger = logging_client.logger("bq-dataset-mover")  

    _print_and_log(cloud_logger, "Starting BQ Dataset Mover")
    _print_and_log(cloud_logger, 'Project: {}'.format(project_id))
    _print_and_log(cloud_logger, 'Dataset: {}'.format(dataset_name))
    _print_and_log(cloud_logger, 'Service Account: {}'.format(sa_email))

    bq_client = bigquery.Client(
                credentials=sa_credentials, project=project_id)
    
    _print_and_log(cloud_logger, "1 Get source dataset details: {}".format(dataset_name))
    source_dataset = bq_client.get_dataset(project_id +'.' + dataset_name)

    if source_dataset is None:
        msg = 'The source dataset does not exist, so we cannot continue'
        cloud_logger.log_text(msg)
        raise SystemExit(msg)
        
    # Get copies of all of the source dataset's IAM, and settings so they
    # can be copied over to the target dataset; details are retrievable
    # only if the corresponding feature is enabled in the configuration
    
    source_dataset_details = DatasetDetails(
        source_dataset=source_dataset)
    
    _print_and_log(cloud_logger, "Dataset details found and set")
    
    bq_dts_client = bigquery_datatransfer_v1.DataTransferServiceClient(credentials=sa_credentials)

    _move_dataset(cloud_logger, project_id, dataset_name, bq_client, bq_dts_client)

    _print_and_log(cloud_logger, 'Completed BQ Dataset Mover')

    
def _get_parsed_args():
    """Parses command line arguments 
    Returns:
        A "Namespace" object. See argparse.ArgumentParser.parse_args() for more details.
    """

    parser = argparse.ArgumentParser(
        description=
        'Moves a BQ dataset within the project')
    parser.add_argument(
        '-d','--dataset_name', help='The name of the dataset to be moved.')
    parser.add_argument(
        '-p','--project_id',
        help='The project id that the dataset is currently in.')
    parser.add_argument(
        '-s','--service_account_key',
        help='The location for service account key json file from the project'
    )
    return parser.parse_args()

def _move_dataset(cloud_logger, project_id, source_dataset, bq_client, bq_dts_client):
    """Main method for doing a dataset move.
    The target bucket will have the same name as the source bucket.

    Args:
        cloud_logger: A GCP logging client instance
        project_id: A Configuration object with all of the config values needed for the script to run
        source_dataset: The bucket object for the original source bucket in the source project
        bq_dts_client: The BQ DTS client object to be used
    """
    
    temp_dataset_name = source_dataset + "_temp"
    
    _print_and_log(cloud_logger, "2 Create temp dataset: {}".format(temp_dataset_name))
    #target_temp_dataset = _create_target_dataset(cloud_logger, project_id, source_dataset, temp_dataset_name, bq_client)
    
    _print_and_log(cloud_logger, "3 Run and wait for BQ DTS job - source to temp")
    _run_and_wait_for_bq_dts_job(bq_dts_client, project_id, source_dataset, temp_dataset_name, cloud_logger)
    
    _print_and_log(cloud_logger, "4 Reconcile datasets")
    #_reconcile_datasets(cloud_logger, project_id, source_dataset, temp_dataset_name, bq_client)
    """
    _print_and_log(cloud_logger, "5 Delete source dataset: {}".format(source_dataset))
    _delete_source_dataset(cloud_logger, source_dataset)
    
    _print_and_log(cloud_logger, "6 Recreate source dataset: {}".format(source_dataset))
    _recreate_source_bucket(cloud_logger, config, source_bucket_details)

    _print_and_log(cloud_logger, "7 Run and wait for BQ DTS job - temp to new source")
    _run_and_wait_for_bq_dts_job(bq_dts_client, project_id, temp_dataset_name, source_dataset, cloud_logger)

    _print_and_log(cloud_logger, "8 Reconcile datasets")
    _reconcile_datasets(cloud_logger, project_id, source_dataset, temp_dataset_name, bq_client)
    
    _print_and_log(cloud_logger, "9 Delete temp dataset: {}".format(source_dataset))
    _delete_source_dataset(cloud_logger, temp_dataset_name)
    """

def _reconcile_datasets(cloud_logger, project_id, first_dataset, second_dataset, bq_client):
    """Creates the temp dataset in the target project

    Args:
        cloud_logger: A GCP logging client instance
        project_id: A Configuration object with all of the config values needed for the script to run
        source_dataset: The details copied from the source bucket that is being moved
        temp_dataset_name: The name of the bucket to create

    Returns:
        The dataset object that has been created in BQ
    """
    _print_and_log(cloud_logger, 'Starting reconciliation between {} and {}'.format(first_dataset,second_dataset))
    _print_and_log(cloud_logger, 'Query dataset {} metrics in project {}'.format(first_dataset, project_id))
    query_job_first_dataset = bq_client.query(
        """
        SELECT COUNT(table_id) as table_count, SUM(row_count) total_rows, SUM(size_bytes) AS total_size 
        FROM `""" + project_id + "." + first_dataset + ".__TABLES__`"
    )
    first_results = query_job_first_dataset.result()
    for row in first_results:
        first_dataset_table_count = row.table_count
        first_dataset_total_rows = row.total_rows
        first_dataset_total_size = row.total_size
        
    _print_and_log(cloud_logger, "{} results = [table_count : {}, total_rows : {}, total_size : {}]"
          .format(first_dataset,str(first_dataset_table_count),str(first_dataset_total_rows),str(first_dataset_total_size))
         )
     
    _print_and_log(cloud_logger, 'Query dataset {} metrics in project {}'.format(second_dataset, project_id))
    query_job_second_dataset = bq_client.query(
        """
        SELECT COUNT(table_id) as table_count, SUM(row_count) total_rows, SUM(size_bytes) AS total_size 
        FROM `""" + project_id + "." + second_dataset + ".__TABLES__`"
    )
    second_results = query_job_second_dataset.result()
    for row in second_results:
        second_dataset_table_count = row.table_count
        second_dataset_total_rows = row.total_rows
        second_dataset_total_size = row.total_size
        
    _print_and_log(cloud_logger, "{} results = [table_count : {}, total_rows : {}, total_size : {}]"
          .format(second_dataset,str(second_dataset_table_count),str(second_dataset_total_rows),str(second_dataset_total_size))
         )
    
    if (first_dataset_table_count == second_dataset_table_count and 
        first_dataset_total_rows == second_dataset_total_rows and
        first_dataset_total_size == second_dataset_total_size):
        _print_and_log(cloud_logger,'Reconciliation complete')
        return
    else:
        msg = 'Reconciliation failed!'
        _print_and_log(cloud_logger,msg)
        raise SystemExit(msg)
    
def _create_target_dataset(cloud_logger, project_id, source_dataset, temp_dataset_name, bq_client):
    """Creates the temp dataset in the target project

    Args:
        cloud_logger: A GCP logging client instance
        project_id: A Configuration object with all of the config values needed for the script to run
        source_dataset: The details copied from the source bucket that is being moved
        temp_dataset_name: The name of the bucket to create

    Returns:
        The dataset object that has been created in BQ
    """

    _print_and_log(cloud_logger,'Creating temp dataset {} in project {}'.format(temp_dataset_name, project_id))
    
    target_dataset = _create_dataset(cloud_logger, project_id, temp_dataset_name, bq_client)
    
    _print_and_log(cloud_logger,'Dataset {} created in target project {}'.format(temp_dataset_name, project_id))
    
    return target_dataset

def _run_and_wait_for_bq_dts_job (bq_dts_client, project_id, source_dataset, temp_dataset_name, cloud_logger):
    #(sts_client, target_project, source_bucket_name, sink_bucket_name, cloud_logger,config,transfer_log_value):
    """Kick off the BQ DTS job and wait for it to complete. Retry if it fails.

    Args:
        sts_client: The STS client object to be used
        target_project: The name of the target project where the STS job will be created
        source_bucket_name: The name of the bucket where the STS job will transfer from
        sink_bucket_name: The name of the bucket where the STS job will transfer to
        cloud_logger: A GCP logging client instance

    Returns:
        True if the STS job completed successfully, False if it failed for any reason
    """

    # Note that this routine is in a @retry decorator, so non-True exits
    # and unhandled exceptions will trigger a retry.

    _print_and_log(cloud_logger,'Moving from dataset {} to {}'.format(source_dataset, temp_dataset_name))
    _print_and_log(cloud_logger,'Creating BQ DTS job')
    bq_dts_run_name = _execute_bq_dts_job(bq_dts_client, project_id,source_dataset, temp_dataset_name, cloud_logger)
    
    print("bq_dts_job_name: {}".format(bq_dts_run_name))

    # Check every 10 seconds until DTS job is complete
    while True:
        job_status = _check_sts_job(cloud_logger, bq_dts_client,
                                        project_id, bq_dts_run_name)
        if job_status != sts_job_status.StsJobStatus.in_progress:
            break
        sleep(10)

    if job_status == sts_job_status.StsJobStatus.success:
        return True

    # Execution will only reach this code if something went wrong with the BQ DTS job
    cloud_logger('There was an unexpected failure with the BQ DTS job. You can view the details in the cloud console.')
    cloud_logger('Waiting for a period of time and then trying again. If you choose to cancel this script, the buckets will need to be manually cleaned up.')
    return False


def _execute_bq_dts_job(bq_dts_client, project_id, source_dataset, temp_dataset_name, cloud_logger):
    """Start the BQ DTS job.

    Args:
        sts_client: The STS client object to be used
        target_project: The name of the target project where the STS job will be created
        source_bucket_name: The name of the bucket where the STS job will transfer from
        sink_bucket_name: The name of the bucket where the STS job will transfer to

    Returns:
        The name of the STS job as a string
    """

    destination_project_id = project_id
    destination_dataset_id = temp_dataset_name
    source_project_id = project_id
    source_dataset_id = source_dataset

    transfer_config = bigquery_datatransfer_v1.TransferConfig(
        destination_dataset_id=destination_dataset_id,
        display_name="Dataset Copy - "+source_dataset_id,
        data_source_id="cross_region_copy",
        params={
            "source_project_id": source_project_id,
            "source_dataset_id": source_dataset_id,
        },
        schedule_options={
            'disable_auto_scheduling': True
        }
    )
    transfer_config_response = bq_dts_client.create_transfer_config(
        parent=bq_dts_client.common_project_path(destination_project_id),
        transfer_config=transfer_config,
    )
    
    _print_and_log(cloud_logger,"Created transfer config: {transfer_config_response.name}")
    now = time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    start_time = Timestamp(seconds=seconds, nanos=nanos)
    
    transfer_runs = bq_dts_client.start_manual_transfer_runs({"parent": transfer_config_response.name, "requested_run_time": start_time})
    
    return transfer_runs.runs[0].name

def _delete_empty_source_bucket(cloud_logger, source_bucket):
    """Delete the empty source bucket

    Args:
        cloud_logger: A GCP logging client instance
        source_bucket: The bucket object for the original source bucket in the source project
    """

    spinner_text = 'Deleting empty source bucket'
    cloud_logger.log_text(spinner_text)
    with yaspin(text=spinner_text) as spinner:
        source_bucket.delete()
        spinner.ok(_CHECKMARK)

def _recreate_source_bucket(cloud_logger, config, source_bucket_details):
    """Now that the original source bucket is deleted, re-create it in the target project

    Args:
        cloud_logger: A GCP logging client instance
        config: A Configuration object with all of the config values needed for the script to run
        source_bucket_details: The details copied from the source bucket that is being moved
    """

    spinner_text = 'Re-creating source bucket in target project'
    _print_and_log(cloud_logger,spinner_text)
    with yaspin(text=spinner_text) as spinner:
        _create_bucket(spinner, cloud_logger, config, config.bucket_name,
                       source_bucket_details)
        spinner.ok(_CHECKMARK)

def _delete_empty_temp_bucket(cloud_logger, target_temp_bucket):
    """Now that the temp bucket is empty, delete it

    Args:
        cloud_logger: A GCP logging client instance
        target_temp_bucket: The GCS bucket object of the target temp bucket
    """

    spinner_text = 'Deleting empty temp bucket'
    cloud_logger.log_text(spinner_text)
    with yaspin(text=spinner_text) as spinner:
        target_temp_bucket.delete()
        spinner.ok(_CHECKMARK)

def _get_project_number(project_id, credentials):
    """Using the project id, get the unique project number for a project.

    Args:
        project_id: The id of the project
        credentials: The credentials to use for accessing the project

    Returns:
        The project number as a string
    """

    crm = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)
    project = crm.projects().get(projectId=project_id).execute(num_retries=5)  # pylint: disable=no-member
    return project['projectNumber']

def _create_dataset (cloud_logger, project_id, temp_dataset_name, bq_client):
    #cloud_logger, config, bucket_name, source_dataset_details):
    """Creates a dataset and replicates all of the settings from source_bucket_details.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        config: A Configuration object with all of the config values needed for the script to run
        bucket_name: The name of the bucket to create
        source_bucket_details: The details copied from the source bucket that is being moved

    Returns:
        The dataset object that has been created in GCS
    """
    
    dataset_id = project_id + "." + temp_dataset_name

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    dataset.location = "asia-east2"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already exists within the project.
    
    dataset = bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
    
    return dataset

def _retry_if_false(result):
    """Return True if we should retry because the function returned False"""
    return result is False

@retry(
    retry_on_result=_retry_if_false,
    wait_exponential_multiplier=4000,
    wait_exponential_max=60000,
    stop_max_attempt_number=5)
def _create_bucket_api_call(spinner, cloud_logger, bucket):
    """Calls the GCS api method to create the bucket.

    The method will attempt to retry up to 5 times if the 503 ServiceUnavailable
    exception is raised.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        bucket: The bucket object to create

    Returns:
        True if the bucket was created, False if a ServiceUnavailable exception was raised

    Raises:
        google.cloud.exceptions.Conflict: The underlying Google Cloud api will raise this error if
            the bucket already exists.
    """

    try:
        bucket.create()
    except exceptions.ServiceUnavailable:
        _write_spinner_and_log(
            spinner, cloud_logger, '503 Service Unavailable error returned.'
            ' Retrying up to 5 times with exponential backoff.')
        return False
    return True

def _update_iam_policies(config, bucket, source_bucket_details):
    """Take the existing IAM, replace the source project number with the target project
    number and then assign the IAM to the new bucket.

    Args:
        config: A Configuration object with all of the config values needed for the script to run
        bucket: The bucket object to update the IAM policies for
        source_bucket_details: The details copied from the source bucket that is being moved
    """

    policy = bucket.get_iam_policy()

    # Update the original policy with the etag for the policy we just got so the update is
    # associated with our get request to make sure no other update overwrites our change
    source_bucket_details.iam_policy.etag = policy.etag
    for role in source_bucket_details.iam_policy:
        for member in source_bucket_details.iam_policy[role]:
            # If a project level role was set, replace it with an identical one for the new project
            if ':' + config.source_project in member:
                new_member = member.replace(config.source_project,
                                            config.target_project)
                source_bucket_details.iam_policy[role].discard(member)
                source_bucket_details.iam_policy[role].add(new_member)

    # Give the target bucket all of the same policies as the source bucket, but with updated
    # project roles
    bucket.set_iam_policy(source_bucket_details.iam_policy)


def _get_sts_iam_account_email(sts_client, project_id):
    """Get the account email that the STS service will run under.

    Args:
        sts_client: The STS client object to be used
        project_id: The id of the project

    Returns:
        The STS service account email as a string
    """

    result = sts_client.googleServiceAccounts().get(
        projectId=project_id).execute(num_retries=5)
    return result['accountEmail']

def _add_target_project_to_kms_key(spinner, cloud_logger, config, kms_key_name):
    """Gives the service_account_email the Encrypter/Decrypter role for the given KMS key.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        config: A Configuration object with all of the config values needed for the script to run
        kms_key_name: The name of the KMS key that the project should be given access to
    """

    kms_client = discovery.build(
        'cloudkms', 'v1', credentials=config.source_project_credentials)

    # Get the current IAM policy and add the new member to it.
    crypto_keys = kms_client.projects().locations().keyRings().cryptoKeys()  # pylint: disable=no-member
    policy_request = crypto_keys.getIamPolicy(resource=kms_key_name)
    policy_response = policy_request.execute(num_retries=5)
    bindings = []
    if 'bindings' in policy_response.keys():
        bindings = policy_response['bindings']
    service_account_email = config.target_storage_client.get_service_account_email()
    members = ['serviceAccount:' + service_account_email]
    bindings.append({
        'role': 'roles/cloudkms.cryptoKeyEncrypterDecrypter',
        'members': members,
    })
    policy_response['bindings'] = bindings

    # Set the new IAM Policy.
    request = crypto_keys.setIamPolicy(
        resource=kms_key_name, body={'policy': policy_response})
    request.execute(num_retries=5)

    _write_spinner_and_log(
        spinner, cloud_logger,
        '{} {} added as Enrypter/Decrypter to key: {}'.format(
            _CHECKMARK, service_account_email, kms_key_name))


def _check_sts_job(spinner, cloud_logger, sts_client, target_project, job_name):
    """Check on the status of the STS job.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        sts_client: The STS client object to be used
        target_project: The name of the target project where the STS job will be created
        job_name: The name of the STS job that was created

    Returns:
        The status of the job as an StsJobStatus enum
    """

    filter_string = (
        '{{"project_id": "{project_id}", "job_names": ["{job_name}"]}}').format(
            project_id=target_project, job_name=job_name)

    result = sts_client.transferOperations().list(
        name='transferOperations', filter=filter_string).execute(num_retries=5)

    if result:
        operation = result['operations'][0]
        metadata = operation['metadata']
        if operation.get('done'):
            if metadata['status'] != 'SUCCESS':
                spinner.fail('X')
                return sts_job_status.StsJobStatus.failed

            _print_sts_counters(spinner, cloud_logger, metadata['counters'],
                                True)
            spinner.ok(_CHECKMARK)
            return sts_job_status.StsJobStatus.success
        else:
            # Update the status of the copy
            if 'counters' in metadata:
                _print_sts_counters(spinner, cloud_logger, metadata['counters'],
                                    False)

    return sts_job_status.StsJobStatus.in_progress

def _print_and_log(cloud_logger, message):
    """Print the message and log it to the cloud.

    Args:
        cloud_logger: A GCP logging client instance
        message: The message to log
    """
    print(message)
    cloud_logger.log_text(message)

class DatasetDetails(object):
    """Holds the details and settings of a dataset."""

    # pylint: disable=attribute-defined-outside-init
    # This is done intentionally so that properties set either in the init or modified in outside
    # code will be forced to follow the skip rules specified on the command line.

    # pylint: disable=too-many-instance-attributes
    # All of these attributes relate to the bucket being copied.

    def __init__(self, source_dataset):
        """Init the class from a source dataset.
        Args:
            conf: the configargparser parsing of command line options
            source_bucket: a google.cloud.storage.Bucket object that the bucket details should be
                copied from.
        """

        # Unless these values are specified on the command line, use the values from the source
        # bucket
        self.access_entries = source_dataset.access_entries
        self.created = source_dataset.created
        self.dataset_id = source_dataset.dataset_id
        self.default_encryption_configuration = source_dataset.default_encryption_configuration
        self.default_partition_expiration_ms = source_dataset.default_partition_expiration_ms
        self.default_table_expiration_ms = source_dataset.default_table_expiration_ms
        self.description = source_dataset.description
        self.etag = source_dataset.etag
        self.friendly_name = source_dataset.friendly_name
        self.full_dataset_id = source_dataset.full_dataset_id
        self.labels = source_dataset.labels
        self.location = source_dataset.location
        self.modified = source_dataset.modified
        self.path = source_dataset.path
        self.project = source_dataset.project
        self.reference = source_dataset.reference
        self.self_link = source_dataset.self_link

        # These properties can be skipped with cmd line params, so use the property setters to
        # make the checks
        #self.iam_policy = source_dataset.get_iam_policy()

    @property
    def iam_policy(self):
        """Get the dataset IAM policy"""
        return self._iam_policy

    @iam_policy.setter
    def iam_policy(self, value):
        self._iam_policy = None if self._skip_iam else value

if __name__ == '__main__':
    main()
