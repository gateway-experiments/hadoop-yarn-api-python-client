# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import argparse
from pprint import pprint

from .auth import SimpleAuth
from .base import get_logger
from .constants import (YarnApplicationState, FinalApplicationStatus,
                        ApplicationState, JobStateInternal)
from . import ResourceManager, NodeManager, HistoryServer, ApplicationMaster

log = get_logger(__name__)


def get_parser():
    parser = argparse.ArgumentParser(
        description='Client for Hadoop® YARN API')

    parser.add_argument('--endpoint', help='API endpoint (https://test.cluster.com:8090)')
    #parser.add_argument('--api_class', help='Please provide api class - rm, hs, nm, am', required=True)
    parser.add_argument('--timeout', help='Request timeout', default=30)
    parser.add_argument('--auth', help='Authentication type', default=None,  choices=['simple', None])
    parser.add_argument('--verify', help='Verify cert or not', default=True)

    subparsers = parser.add_subparsers()
    populate_resource_manager_arguments(subparsers)
    populate_node_manager_arguments(subparsers)
    populate_application_master_arguments(subparsers)
    populate_history_server_arguments(subparsers)

    return parser


def create_parsers(subparsers_instance, module_class, module_name, listing_of_apis):
    for api in listing_of_apis:
        _help_message = module_name + " " + api.replace("_", " ").title() + " API"
        _new_parser = subparsers_instance.add_parser(
            api, help=_help_message
        )
        _new_parser.set_defaults(method=api)
        _method = getattr(module_class, api)
        for _arg in _method.__code__.co_varnames[:_method.__code__.co_argcount]:
            if _arg != 'self':
                _new_parser.add_argument(_arg)


def populate_resource_manager_arguments(subparsers):
    rm_parser = subparsers.add_parser(
        'rm', help='ResourceManager REST API\'s')
    rm_parser.set_defaults(api_class=ResourceManager)

    rm_subparsers = rm_parser.add_subparsers()

    listing_of_apis = [
        'cluster_information',
        'cluster_metrics',
        'cluster_scheduler',
        'cluster_applications',
        'cluster_application_statistics',
        'cluster_application',
        'cluster_application_attempts',
        'cluster_application_attempt_info',
        'cluster_application_attempt_containers',
        'cluster_application_attempt_container_info',
        'cluster_application_state',
        'cluster_application_kill',
        'cluster_nodes',
        'cluster_node',
        'cluster_node_update_resource',
        'cluster_submit_application',
        'cluster_new_application',
        'cluster_get_application_queue',
        'cluster_change_application_queue',
        'cluster_get_application_priority',
        'cluster_change_application_priority',
        'cluster_node_container_memory',
        'cluster_scheduler_queue',
        'cluster_scheduler_queue_availability',
        'cluster_queue_partition',
        'cluster_reservations',
        'cluster_new_delegation_token',
        'cluster_renew_delegation_token',
        'cluster_cancel_delegation_token',
        'cluster_new_reservation',
        'cluster_submit_reservation',
        'cluster_update_reservation',
        'cluster_delete_reservation',
        'cluster_application_timeouts',
        'cluster_application_timeout',
        'cluster_update_application_timeout',
        'cluster_scheduler_conf_mutation',
        'cluster_modify_scheduler_conf_mutation',
        'cluster_container_signal',
        'scheduler_activities',
        'application_activities'
    ]

    create_parsers(rm_subparsers, ResourceManager, "Resource Manager", listing_of_apis)


def populate_node_manager_arguments(subparsers):
    nm_parser = subparsers.add_parser(
        'nm', help='NodeManager REST API\'s')
    nm_parser.set_defaults(api_class=NodeManager)

    nm_subparsers = nm_parser.add_subparsers()

    listing_of_apis = [
        'node_information',
        'node_applications',
        'node_application',
        'node_containers',
        'node_container',
        'auxiliary_services',
        'auxiliary_services_update'
    ]

    create_parsers(nm_subparsers, NodeManager, "Node Manager", listing_of_apis)


def populate_application_master_arguments(subparsers):
    am_parser = subparsers.add_parser(
        'am', help='MapReduce Application Master REST API\'s')
    am_parser.set_defaults(api_class=ApplicationMaster)

    am_subparsers = am_parser.add_subparsers()

    listing_of_apis = [
        'application_information',
        'jobs',
        'job',
        'job_attempts',
        'job_counters',
        'job_conf',
        'job_tasks',
        'job_task',
        'task_counters',
        'task_attempts',
        'task_attempt',
        'task_attempt_state',
        'task_attempt_state_kill',
        'task_attempt_counters'
    ]

    create_parsers(am_subparsers, ApplicationMaster, "Application Master", listing_of_apis)


def populate_history_server_arguments(subparsers):
    hs_parser = subparsers.add_parser(
        'hs', help='History Server REST API\'s')
    hs_parser.set_defaults(api_class=HistoryServer)

    hs_subparsers = hs_parser.add_subparsers()

    listing_of_apis = [
        'application_information',
        'jobs',
        'job',
        'job_attempts',
        'job_counters',
        'job_conf',
        'job_tasks',
        'job_task',
        'task_counters',
        'task_attempts',
        'task_attempt',
        'task_attempt_counters'
    ]

    create_parsers(hs_subparsers, HistoryServer, "History Server", listing_of_apis)


def main():
    parser = get_parser()
    opts = parser.parse_args()

    class_kwargs = {}
    # Only ResourceManager supports HA
    if opts.endpoint:
        if opts.api_class == ResourceManager:
            class_kwargs['service_endpoints'] = opts.endpoint.split(",")
        else:
            class_kwargs['service_endpoint'] = opts.endpoint

    # CLI requires some special accommodation for Auth - custom class imports
    if opts.auth:
        # Currenly only hadoop's SimpleAuth and none are supported out of the box
        if opts.auth == 'simple':
            class_kwargs['auth'] = SimpleAuth()
        else:
            raise Exception(
                "This auth mentod is not supported by CLI, please write your own python script if needed"
            )

    api = opts.api_class(**class_kwargs)
    # Construct positional arguments for method
    if 'method_args' in opts:
        method_args = [getattr(opts, arg) for arg in opts.method_args]
    else:
        method_args = []
    # Construct key arguments for method
    if 'method_kwargs' in opts:
        method_kwargs = dict((key, getattr(opts, key)) for key in opts.method_kwargs)
    else:
        method_kwargs = {}
    response = getattr(api, opts.method)(*method_args, **method_kwargs)
    pprint(response.data)
