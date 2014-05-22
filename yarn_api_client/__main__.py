# -*- coding: utf-8 -*-
import argparse
import logging
from pprint import pprint
import sys

from .constants import YarnApplicationState, FinalApplicationStatus
from .resource_manager import ResourceManager

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def get_parser():
    parser = argparse.ArgumentParser(
        description=u'Client for Hadoop® YARN API')

    parser.add_argument('--host', help=u'API host')
    parser.add_argument('--port', help=u'API port')

    subparsers = parser.add_subparsers()
    populate_resource_manager_arguments(subparsers)
    populate_node_manager_arguments(subparsers)
    populate_application_master_arguments(subparsers)
    populate_history_server_arguments(subparsers)

    return parser


def populate_resource_manager_arguments(subparsers):
    rm_parser = subparsers.add_parser(
        'rm', help=u'ResourceManager REST API\'s')
    rm_parser.set_defaults(api_class=ResourceManager)

    rm_subparsers = rm_parser.add_subparsers()

    ci_parser = rm_subparsers.add_parser(
        'info', help=u'Cluster Information API')
    ci_parser.set_defaults(method='cluster_information')

    cm_parser = rm_subparsers.add_parser(
        'metrics', help=u'Cluster Metrics API')
    cm_parser.set_defaults(method='cluster_metrics')

    cs_parser = rm_subparsers.add_parser(
        'scheduler', help=u'Cluster Scheduler API')
    cs_parser.set_defaults(method='cluster_scheduler')

    cas_parser = rm_subparsers.add_parser(
        'apps', help=u'Cluster Applications API')
    cas_parser.add_argument('--state',
                            help=u'states of the applications',
                            choices=dict(YarnApplicationState).keys())
    cas_parser.add_argument('--final-status',
                            choices=dict(FinalApplicationStatus).keys())
    cas_parser.add_argument('--user')
    cas_parser.add_argument('--queue')
    cas_parser.add_argument('--limit')
    cas_parser.add_argument('--started-time-begin')
    cas_parser.add_argument('--started-time-end')
    cas_parser.add_argument('--finished-time-begin')
    cas_parser.add_argument('--finished-time-end')
    cas_parser.set_defaults(method='cluster_applications')
    cas_parser.set_defaults(method_kwargs=[
            'state', 'user', 'queue', 'limit',
            'started_time_begin', 'started_time_end', 'finished_time_begin',
            'finished_time_end', 'final_status'])

    ca_parser = rm_subparsers.add_parser(
        'app', help=u'Cluster Application API')
    ca_parser.add_argument('application_id')
    ca_parser.set_defaults(method='cluster_application')
    ca_parser.set_defaults(method_args=['application_id'])

    caa_parser = rm_subparsers.add_parser(
        'app_attempts', help=u'Cluster Application Attempts API')
    caa_parser.add_argument('application_id')
    caa_parser.set_defaults(method='cluster_application_attempts')
    caa_parser.set_defaults(method_args=['application_id'])

    cns_parser = rm_subparsers.add_parser(
        'nodes', help=u'Cluster Nodes API')
    cns_parser.add_argument('--state', help=u'the state of the node')
    cns_parser.add_argument('--healthy', help='true or false')
    cns_parser.set_defaults(method='cluster_nodes')
    cns_parser.set_defaults(method_kargs=['state', 'healthy'])

    cn_parser = rm_subparsers.add_parser(
        'node', help=u'Cluster Node API')
    cn_parser.add_argument('node_id')
    cn_parser.set_defaults(method='cluster_node')
    cn_parser.set_defaults(method_args=['node_id'])


def populate_node_manager_arguments(subparsers):
    nm_parser = subparsers.add_parser(
        'nm', help=u'NodeManager REST API\'s')


def populate_application_master_arguments(subparsers):
    am_parser = subparsers.add_parser(
        'am', help=u'MapReduce Application Master REST API\'s')


def populate_history_server_arguments(subparsers):
    hs_parser = subparsers.add_parser(
        'hs', help=u'History Server REST API\'s')


if __name__ == '__main__':
    parser = get_parser()
    opts = parser.parse_args()

    api = opts.api_class(opts.host, opts.port)
    # Construct positional arguments for method
    if 'method_args' in opts:
        method_args = [getattr(opts, arg) for arg in opts.method_args]
    else:
        method_args = []
    # Construce key arguments for method
    if 'method_kwargs' in opts:
        method_kwargs = {key: getattr(opts, key) for key in opts.method_kwargs}
    else:
        method_kwargs = {}
    response = getattr(api, opts.method)(*method_args, **method_kwargs)
    pprint(response.data)
