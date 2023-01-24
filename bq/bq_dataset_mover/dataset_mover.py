#!/usr/bin/env python
"""Script to move a bucket, all settings and data from one project to another."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime
import json
from time import sleep
from retrying import retry
from yaspin import yaspin
import argparse

from google.api_core import iam as api_core_iam
from google.cloud import exceptions
from google.cloud import bigquery
from google.cloud import logging
from google.cloud import bigquery_datatransfer
from googleapiclient import discovery
from google.oauth2 import service_account

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

    cloud_logger.log_text("Starting BQ Dataset Mover")
    _print_and_log(cloud_logger, 'Project: {}'.format(project_id))
    _print_and_log(cloud_logger, 'Dataset: {}'.format(dataset_name))
    _print_and_log(cloud_logger, 'Service Account: {}'.format(sa_email))

    bq_client = bigquery.Client(
                credentials=sa_credentials, project=project_id)
    source_dataset = bq_client.get_dataset(  
        project_id+'.'+dataset_name)

    if source_dataset is None:
        msg = 'The source dataset does not exist, so we cannot continue'
        cloud_logger.log_text(msg)
        raise SystemExit(msg)

    # Get copies of all of the source dataset's IAM, and settings so they
    # can be copied over to the target dataset; details are retrievable
    # only if the corresponding feature is enabled in the configuration
    #source_bucket_details = bucket_details.BucketDetails(
    #    conf=parsed_args, source_bucket=source_bucket)
    #transfer_log_value=_check_log_values(cloud_logger, config)
    
    bq_dts_client = bigquery_datatransfer.DataTransferServiceClient()

    _move_bucket(cloud_logger, project_id, source_dataset, bq_client, bq_dts_client)

    cloud_logger.log_text('Completed BQ Dataset Mover')

    
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
    target_temp_dataset = _create_target_dataset(cloud_logger, project_id, source_dataset, temp_dataset_name, bq_client)
    
    """
    bq_dts_account_email = _assign_bqdts_permissions(cloud_logger, sts_client, config, target_temp_bucket)
    _run_and_wait_for_sts_job(sts_client, config.target_project,
                              config.bucket_name, config.temp_bucket_name,
                              cloud_logger,config,transfer_log_value)

    _delete_empty_source_bucket(cloud_logger, source_bucket)
    _recreate_source_bucket(cloud_logger, config, source_bucket_details)
    _assign_sts_permissions_to_new_bucket(cloud_logger, bq_dts_account_email,
                                          config)
    _run_and_wait_for_sts_job(sts_client, config.target_project,
                              config.temp_bucket_name, config.bucket_name,
                              cloud_logger,config,transfer_log_value)

    _delete_empty_temp_bucket(cloud_logger, target_temp_bucket)
    _remove_sts_permissions(cloud_logger, bq_dts_account_email, config,
                            config.bucket_name)
    """
        
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

    cloud_logger.log_text('Creating temp dataset {} in project {}'.format(temp_dataset_name, project_id))
    
    target_dataset = _create_dataset(cloud_logger, project_id, temp_dataset_name, bq_client)
    
    cloud_logger.log_text('Dataset {} created in target project {}'.format(temp_dataset_name, project_id))
    
    return target_dataset


def _assign_sts_permissions(cloud_logger, sts_client, config,
                            target_temp_bucket):
    """Assign the required STS permissions to the source/temp bucket

    Args:
        cloud_logger: A GCP logging client instance
        sts_client: The STS client object to be used
        config: A Configuration object with all of the config values needed for the script to run
        target_temp_bucket: The bucket object for the temp bucket in the target project

    Returns:
        The email account of the STS account
    """

    spinner_text = 'Assigning STS permissions to source/temp buckets'
    cloud_logger.log_text(spinner_text)
    with yaspin(text=spinner_text) as spinner:
        sts_account_email = _get_sts_iam_account_email(sts_client,
                                                       config.target_project)
        _write_spinner_and_log(
            spinner, cloud_logger,
            'STS service account for IAM usage: {}'.format(sts_account_email))
        _assign_sts_iam_roles(sts_account_email, config.source_storage_client,
                              config.source_project, config.bucket_name, True)
        _assign_sts_iam_roles(sts_account_email, config.target_storage_client,
                              config.target_project, target_temp_bucket.name,
                              True)
        spinner.ok(_CHECKMARK)
        return sts_account_email


def _assign_sts_permissions_to_new_bucket(cloud_logger, sts_account_email,
                                          config):
    """Assign the required STS permissions to the new source bucket in the target project

    Args:
        cloud_logger: A GCP logging client instance
        sts_account_email: The email account of the STS account
        config: A Configuration object with all of the config values needed for the script to run
    """

    spinner_text = 'Assigning STS permissions to new source bucket'
    cloud_logger.log_text(spinner_text)
    with yaspin(text=spinner_text) as spinner:
        _assign_sts_iam_roles(sts_account_email, config.target_storage_client,
                              config.target_project, config.bucket_name, False)
        spinner.ok(_CHECKMARK)


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
    cloud_logger.log_text(spinner_text)
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


def _remove_sts_permissions(cloud_logger, sts_account_email, config,
                            bucket_name):
    """Remove the STS permissions from the new source bucket in the target project

    Args:
        cloud_logger: A GCP logging client instance
        sts_account_email: The email account of the STS account
        config: A Configuration object with all of the config values needed for the script to run
        bucket_name: The name of the bucket to remove the permissions from
    """

    spinner_text = 'Removing STS permissions from bucket {}'.format(bucket_name)
    cloud_logger.log_text(spinner_text)
    with yaspin(text=spinner_text) as spinner:
        _remove_sts_iam_roles(sts_account_email, config.target_storage_client,
                              bucket_name)
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


def _create_dataset (cloud_logger, project_id, temp_dataset_name, bq_client)
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
    
    dataset_id = project_id + "." temp_dataset_name

    # Construct a full Dataset object to send to the API.
    dataset = bq_client.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "asia-east2"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already exists within the project.
    
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    
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


def _update_acl_entities(config, source_entities):
    """Update the source ACL entities.

    Take the existing ACLs, replace the source project number with the target project number and
    then assign the ACLs to the new bucket.

    Args:
        config: A Configuration object with all of the config values needed for the script to run
        source_entities: The existing ACL entities of the bucket

    Returns:
        The list of ACLs with project numbers replaced
    """

    source_project_number = _get_project_number(
        config.source_project, config.source_project_credentials)
    target_project_number = _get_project_number(
        config.target_project, config.target_project_credentials)

    new_acl = storage.acl.ACL()
    new_acl.loaded = True
    # If an entity is for the source project, replace it with the identical one for the new
    # project
    for entity in source_entities:
        # Skip it if it has no identifier
        if not hasattr(entity, 'identifier'):
            continue

        # Skip it if the identifier is empty
        if entity.identifier is None:
            continue

        # Skip it if the identifier doesn't contain the source project number
        if '-' + source_project_number not in entity.identifier:
            continue

        # Replace the source project number with the target project number and add the entity
        entity.identifier = entity.identifier.replace(source_project_number,
                                                      target_project_number)
        new_acl.add_entity(entity)

    return new_acl


def _update_notifications(spinner, cloud_logger, config, notifications, bucket):
    """Update the notifications on the target bucket to match those from the source bucket.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        config: A Configuration object with all of the config values needed for the script to run
        notifications: A list of notifications to add to the bucket
        bucket: The bucket object to update the notifications for
    """

    for item in notifications:
        # Give target project service account access to publish to source project topic
        _assign_target_project_to_topic(spinner, cloud_logger, config,
                                        item.topic_name, item.topic_project)

        notification = storage.notification.BucketNotification(
            bucket,
            item.topic_name,
            topic_project=item.topic_project,
            custom_attributes=item.custom_attributes,
            event_types=item.event_types,
            blob_name_prefix=item.blob_name_prefix,
            payload_format=item.payload_format)
        notification.create()


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


def _assign_sts_iam_roles(sts_email, storage_client, project_name, bucket_name,
                          assign_viewer):
    """Assign roles to the STS service account that will be required to interact with the bucket.

    Args:
        sts_email: The email address for the STS service account
        storage_client: The storage client object used to access GCS
        project_name: The name of the project
        bucket_name: The name of the bucket
        assign_viewer: True if we should also assign the Object Viewer/LegacyReader roles
    """

    account = 'serviceAccount:' + sts_email
    bucket = storage_client.bucket(bucket_name, project_name)
    policy = bucket.get_iam_policy()
    policy['roles/storage.legacyBucketWriter'].add(account)
    if assign_viewer:
        policy[iam.STORAGE_OBJECT_VIEWER_ROLE].add(account)
        policy['roles/storage.legacyBucketReader'].add(account)

    bucket.set_iam_policy(policy)


def _remove_sts_iam_roles(sts_email, storage_client, bucket_name):
    """Remove the roles that were assigned for the STS service account.

    Args:
        sts_email: The email address for the STS service account
        storage_client: The storage client object used to access GCS
        bucket_name: The name of the bucket
    """

    account = 'serviceAccount:' + sts_email
    bucket = storage_client.bucket(bucket_name)
    policy = bucket.get_iam_policy()
    policy['roles/storage.legacyBucketWriter'].discard(account)
    bucket.set_iam_policy(policy)


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


def _assign_target_project_to_topic(spinner, cloud_logger, config, topic_name,
                                    topic_project):
    """Gives the service_account_email the Publisher role for the topic.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        config: A Configuration object with all of the config values needed for the script to run
        topic_name: The name of the topic that the target project should be assigned to
        topic_project: The name of the project that the topic belongs to
    """

    client = pubsub.PublisherClient(
        credentials=config.source_project_credentials)
    topic_path = client.topic_path(topic_project, topic_name)  # pylint: disable=no-member
    policy = client.get_iam_policy(topic_path)  # pylint: disable=no-member

    service_account_email = config.target_storage_client.get_service_account_email()
    policy.bindings.add(
        role='roles/pubsub.publisher',
        members=['serviceAccount:' + service_account_email])

    client.set_iam_policy(topic_path, policy)  # pylint: disable=no-member

    _write_spinner_and_log(
        spinner, cloud_logger, '{} {} added as a Publisher to topic: {}'.format(
            _CHECKMARK, service_account_email, topic_name))


@retry(
    retry_on_result=_retry_if_false,
    wait_exponential_multiplier=10000,
    wait_exponential_max=120000,
    stop_max_attempt_number=10)
def _run_and_wait_for_sts_job(sts_client, target_project, source_bucket_name,
                              sink_bucket_name, cloud_logger,config,transfer_log_value):
    """Kick off the STS job and wait for it to complete. Retry if it fails.

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

    msg = 'Moving from bucket {} to {}'.format(source_bucket_name,
                                               sink_bucket_name)
    _print_and_log(cloud_logger, msg)

    spinner_text = 'Creating STS job'
    cloud_logger.log_text(spinner_text)
    with yaspin(text=spinner_text) as spinner:
        sts_job_name = _execute_sts_job(sts_client, target_project,
                                        source_bucket_name, sink_bucket_name,config,transfer_log_value)
        spinner.ok(_CHECKMARK)

    # Check every 10 seconds until STS job is complete
    with yaspin(text='Checking STS job status') as spinner:
        while True:
            job_status = _check_sts_job(spinner, cloud_logger, sts_client,
                                        target_project, sts_job_name)
            if job_status != sts_job_status.StsJobStatus.in_progress:
                break
            sleep(10)

    if job_status == sts_job_status.StsJobStatus.success:
  
        return True

    # Execution will only reach this code if something went wrong with the STS job
    _print_and_log(
        cloud_logger,
        'There was an unexpected failure with the STS job. You can view the'
        ' details in the cloud console.')
    _print_and_log(
        cloud_logger,
        'Waiting for a period of time and then trying again. If you choose to'
        ' cancel this script, the buckets will need to be manually cleaned up.')
    return False


def _execute_sts_job(sts_client, target_project, source_bucket_name,
                     sink_bucket_name,config,transfer_log_value):
    """Start the STS job.

    Args:
        sts_client: The STS client object to be used
        target_project: The name of the target project where the STS job will be created
        source_bucket_name: The name of the bucket where the STS job will transfer from
        sink_bucket_name: The name of the bucket where the STS job will transfer to

    Returns:
        The name of the STS job as a string
    """

    now = datetime.date.today()
    if config.bucket_name == sink_bucket_name:
        time_preserved = None
    else:
        if config.preserve_custom_time == None:
            time_preserved = None

        elif config.preserve_custom_time == "TIME_CREATED_PRESERVE_AS_CUSTOM_TIME":
            time_preserved= config.preserve_custom_time

        elif config.preserve_custom_time == "TIME_CREATED_SKIP":
            time_preserved = config.preserve_custom_time
        
        elif config.preserve_custom_time == "TIME_CREATED_UNSPECIFIED":
            time_preserved = config.preserve_custom_time

        else:
            msg = 'Time created value is not available'
            raise SystemExit(msg)	
    
    transfer_job = {
        'description':
        'Move bucket {} to {} in project {}'.format(
            source_bucket_name, sink_bucket_name, target_project),
        'status': 'ENABLED',
        'projectId': target_project,
        'schedule': {
            'scheduleStartDate': {
                'day': now.day - 1,
                'month': now.month,
                'year': now.year
            },
            'scheduleEndDate': {
                'day': now.day - 1,
                'month': now.month,
                'year': now.year
            }
        },
        'transferSpec': {
            'gcsDataSource': {
                'bucketName': source_bucket_name
            },
            'gcsDataSink': {
                'bucketName': sink_bucket_name
            },
            "transferOptions": {
                "deleteObjectsFromSourceAfterTransfer": True,
                "metadataOptions": {
                    "timeCreated": time_preserved
                }         
            }
        }
    }
    transfer_job["loggingConfig"]=transfer_log_value   
    result = sts_client.transferJobs().create(body=transfer_job).execute(
        num_retries=5)
    return result['name']


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


def _print_sts_counters(spinner, cloud_logger, counters, is_job_done):
    """Print out the current STS job counters.

    Args:
        spinner: The spinner displayed in the console
        cloud_logger: A GCP logging client instance
        counters: The counters object returned as part of the STS job status query
        is_job_done: If True, print out the final counters instead of just the in progress ones
    """

    if counters:
        bytes_copied_to_sink = int(counters.get('bytesCopiedToSink', '0'))
        objects_copied_to_sink = int(counters.get('objectsCopiedToSink', '0'))
        bytes_found_from_source = int(counters.get('bytesFoundFromSource', '0'))
        objects_found_from_source = int(counters.get('objectsFoundFromSource', '0'))
        bytes_deleted_from_source = int(counters.get('bytesDeletedFromSource', '0'))
        objects_deleted_from_source = int(counters.get('objectsDeletedFromSource','0'))

        if is_job_done:
            byte_status = (bytes_copied_to_sink == bytes_found_from_source ==
                           bytes_deleted_from_source)
            object_status = (objects_copied_to_sink == objects_found_from_source
                             == objects_deleted_from_source)

            if byte_status and object_status:
                new_text = 'Success! STS job copied {} bytes in {} objects'.format(
                    bytes_copied_to_sink, objects_copied_to_sink)
            else:
                new_text = (
                    'Error! STS job copied {} of {} bytes in {} of {} objects and deleted'
                    ' {} bytes and {} objects').format(
                        bytes_copied_to_sink, bytes_found_from_source,
                        objects_copied_to_sink, objects_found_from_source,
                        bytes_deleted_from_source, objects_deleted_from_source)

            if spinner.text != new_text:
                spinner.write(spinner.text)
                spinner.text = new_text
                cloud_logger.log_text(new_text)
        else:
            if bytes_copied_to_sink > 0 and objects_copied_to_sink > 0:
                byte_percent = '{:.0%}'.format(
                    float(bytes_copied_to_sink) /
                    float(bytes_found_from_source))
                object_percent = '{:.0%}'.format(
                    float(objects_copied_to_sink) /
                    float(objects_found_from_source))
                spinner.write(spinner.text)
                new_text = '{} of {} bytes ({}) copied in {} of {} objects ({})'.format(
                    bytes_copied_to_sink, bytes_found_from_source, byte_percent,
                    objects_copied_to_sink, objects_found_from_source,
                    object_percent)
                spinner.text = new_text
                cloud_logger.log_text(new_text)


def _print_and_log(cloud_logger, message):
    """Print the message and log it to the cloud.

    Args:
        cloud_logger: A GCP logging client instance
        message: The message to log
    """
   
    cloud_logger.log_text(message)


def _write_spinner_and_log(spinner, cloud_logger, message):
    """Write the message to the spinner and log it to the cloud.

    Args:
        spinner: The spinner object to write the message to
        cloud_logger: A GCP logging client instance
        message: The message to print and log
    """
    spinner.write(message)
    cloud_logger.log_text(message)


if __name__ == '__main__':
    main()
