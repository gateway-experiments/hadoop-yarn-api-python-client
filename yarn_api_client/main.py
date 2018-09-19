# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import argparse
import logging
from pprint import pprint
import sys

from .constants import (YarnApplicationState, FinalApplicationStatus,
                        ApplicationState, JobStateInternal)
from . import ResourceManager, NodeManager, HistoryServer, ApplicationMaster

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_parser():
    parser = argparse.ArgumentParser(
        description='Client for Hadoop® YARN API')

    parser.add_argument('--host', help='API host')
    parser.add_argument('--port', help='API port')

    subparsers = parser.add_subparsers()
    populate_resource_manager_arguments(subparsers)
    populate_node_manager_arguments(subparsers)
    populate_application_master_arguments(subparsers)
    populate_history_server_arguments(subparsers)

    return parser


def populate_resource_manager_arguments(subparsers):
    rm_parser = subparsers.add_parser(
        'rm', help='ResourceManager REST API\'s')
    rm_parser.set_defaults(api_class=ResourceManager)

    rm_subparsers = rm_parser.add_subparsers()

    ci_parser = rm_subparsers.add_parser(
        'info', help='Cluster Information API')
    ci_parser.set_defaults(method='cluster_information')

    cm_parser = rm_subparsers.add_parser(
        'metrics', help='Cluster Metrics API')
    cm_parser.set_defaults(method='cluster_metrics')

    cs_parser = rm_subparsers.add_parser(
        'scheduler', help='Cluster Scheduler API')
    cs_parser.set_defaults(method='cluster_scheduler')

    cas_parser = rm_subparsers.add_parser(
        'apps', help='Cluster Applications API')
    cas_parser.add_argument('--state',
                            help='states of the applications',
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
        'app', help='Cluster Application API')
    ca_parser.add_argument('application_id')
    ca_parser.set_defaults(method='cluster_application')
    ca_parser.set_defaults(method_args=['application_id'])

    caa_parser = rm_subparsers.add_parser(
        'app_attempts', help='Cluster Application Attempts API')
    caa_parser.add_argument('application_id')
    caa_parser.set_defaults(method='cluster_application_attempts')
    caa_parser.set_defaults(method_args=['application_id'])

    cns_parser = rm_subparsers.add_parser(
        'nodes', help='Cluster Nodes API')
    cns_parser.add_argument('--state', help='the state of the node')
    cns_parser.add_argument('--healthy', help='true or false')
    cns_parser.set_defaults(method='cluster_nodes')
    cns_parser.set_defaults(method_kargs=['state', 'healthy'])

    cn_parser = rm_subparsers.add_parser(
        'node', help='Cluster Node API')
    cn_parser.add_argument('node_id')
    cn_parser.set_defaults(method='cluster_node')
    cn_parser.set_defaults(method_args=['node_id'])


def populate_node_manager_arguments(subparsers):
    nm_parser = subparsers.add_parser(
        'nm', help='NodeManager REST API\'s')
    nm_parser.set_defaults(api_class=NodeManager)

    nm_subparsers = nm_parser.add_subparsers()

    ni_parser = nm_subparsers.add_parser(
        'info', help='NodeManager Information API')
    ni_parser.set_defaults(method='node_information')

    nas_parser = nm_subparsers.add_parser(
        'apps', help='Applications API')
    nas_parser.add_argument('--state',
                            help='application state',
                            choices=dict(ApplicationState).keys())
    nas_parser.add_argument('--user',
                            help='user name')
    nas_parser.set_defaults(method='node_applications')
    nas_parser.set_defaults(method_kwargs=['state', 'user'])

    na_parser = nm_subparsers.add_parser(
        'app', help='Application API')
    na_parser.add_argument('application_id')
    na_parser.set_defaults(method='node_application')
    na_parser.set_defaults(method_args=['application_id'])

    ncs_parser = nm_subparsers.add_parser(
        'containers', help='Containers API')
    ncs_parser.set_defaults(method='node_containers')

    nc_parser = nm_subparsers.add_parser(
        'container', help='Container API')
    nc_parser.add_argument('container_id')
    nc_parser.set_defaults(method='node_container')
    nc_parser.set_defaults(method_args=['container_id'])


def populate_application_master_arguments(subparsers):
    am_parser = subparsers.add_parser(
        'am', help='MapReduce Application Master REST API\'s')
    am_parser.set_defaults(api_class=ApplicationMaster)
    am_parser.add_argument('application_id')

    # TODO: not implemented


def populate_history_server_arguments(subparsers):
    hs_parser = subparsers.add_parser(
        'hs', help='History Server REST API\'s')
    hs_parser.set_defaults(api_class=HistoryServer)

    hs_subparsers = hs_parser.add_subparsers()

    hi_parser = hs_subparsers.add_parser(
        'info', help='History Server Information API')
    hi_parser.set_defaults(method='application_information')

    hjs_parser = hs_subparsers.add_parser(
        'jobs', help='Jobs API')
    hjs_parser.add_argument('--state',
                            help='states of the applications',
                            choices=dict(JobStateInternal).keys())
    hjs_parser.add_argument('--user')
    hjs_parser.add_argument('--queue')
    hjs_parser.add_argument('--limit')
    hjs_parser.add_argument('--started-time-begin')
    hjs_parser.add_argument('--started-time-end')
    hjs_parser.add_argument('--finished-time-begin')
    hjs_parser.add_argument('--finished-time-end')
    hjs_parser.set_defaults(method='jobs')
    hjs_parser.set_defaults(method_kwargs=[
            'state', 'user', 'queue', 'limit',
            'started_time_begin', 'started_time_end', 'finished_time_begin',
            'finished_time_end'])

    hj_parser = hs_subparsers.add_parser('job', help='Job API')
    hj_parser.add_argument('job_id')
    hj_parser.set_defaults(method='job')
    hj_parser.set_defaults(method_args=['job_id'])

    hja_parser = hs_subparsers.add_parser(
        'job_attempts', help='Job Attempts API')
    hja_parser.add_argument('job_id')
    hja_parser.set_defaults(method='job_attempts')
    hja_parser.set_defaults(method_args=['job_id'])

    hjc_parser = hs_subparsers.add_parser(
        'job_counters', help='Job Counters API')
    hjc_parser.add_argument('job_id')
    hjc_parser.set_defaults(method='job_counters')
    hjc_parser.set_defaults(method_args=['job_id'])

    hjcn_parser = hs_subparsers.add_parser(
        'job_conf', help='Job Conf API')
    hjcn_parser.add_argument('job_id')
    hjcn_parser.set_defaults(method='job_conf')
    hjcn_parser.set_defaults(method_args=['job_id'])

    hts_parser = hs_subparsers.add_parser(
        'tasks', help='Tasks API')
    hts_parser.add_argument('job_id')
    hts_parser.add_argument('--type', choices=['m', 'r'],
                            help=('type of task, m for map task '
                                  'or r for reduce task.'))
    hts_parser.set_defaults(method='job_tasks')
    hts_parser.set_defaults(method_args=['job_id'])
    hts_parser.set_defaults(method_kwargs=['type'])

    ht_parser = hs_subparsers.add_parser(
        'task', help='Task API')
    ht_parser.add_argument('job_id')
    ht_parser.add_argument('task_id')
    ht_parser.set_defaults(method='job_task')
    ht_parser.set_defaults(method_args=['job_id', 'task_id'])

    htc_parser = hs_subparsers.add_parser(
        'task_counters', help='Task Counters API')
    htc_parser.add_argument('job_id')
    htc_parser.add_argument('task_id')
    htc_parser.set_defaults(method='task_counters')
    htc_parser.set_defaults(method_args=['job_id', 'task_id'])

    htas_parser = hs_subparsers.add_parser(
        'task_attempts', help='Task Attempts API')
    htas_parser.add_argument('job_id')
    htas_parser.add_argument('task_id')
    htas_parser.set_defaults(method='task_attempts')
    htas_parser.set_defaults(method_args=['job_id', 'task_id'])

    hta_parser = hs_subparsers.add_parser(
        'task_attempt', help='Task Attempt API')
    hta_parser.add_argument('job_id')
    hta_parser.add_argument('task_id')
    hta_parser.add_argument('attempt_id')
    hta_parser.set_defaults(method='task_attempt')
    hta_parser.set_defaults(method_args=['job_id', 'task_id', 'attempt_id'])

    htac_parser = hs_subparsers.add_parser(
        'task_attempt_counters', help='Task Attempt Counters API')
    htac_parser.add_argument('job_id')
    htac_parser.add_argument('task_id')
    htac_parser.add_argument('attempt_id')
    htac_parser.set_defaults(method='task_attempt_counters')
    htac_parser.set_defaults(method_args=['job_id', 'task_id', 'attempt_id'])


def main():
    parser = get_parser()
    opts = parser.parse_args()

    class_kwargs = {}
    if opts.host is not None:
        class_kwargs['address'] = opts.host
    if opts.port is not None:
        class_kwargs['port'] = opts.port

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
