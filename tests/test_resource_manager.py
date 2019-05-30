# -*- coding: utf-8 -*-
from mock import patch
from tests import TestCase

from yarn_api_client.resource_manager import ResourceManager
from yarn_api_client.errors import IllegalArgumentError


@patch('yarn_api_client.resource_manager.ResourceManager.request')
class ResourceManagerTestCase(TestCase):
    @patch('yarn_api_client.resource_manager.check_is_active_rm')
    def setUp(self, check_is_active_rm_mock):
        check_is_active_rm_mock.return_value = True
        self.rm = ResourceManager(['localhost'])

    @patch('yarn_api_client.resource_manager.get_resource_manager_endpoint')
    def test__init__(self, get_config_mock, request_mock):
        get_config_mock.return_value = "localhost"
        ResourceManager()
        get_config_mock.assert_called_with(30)

    def test_cluster_information(self, request_mock):
        self.rm.cluster_information()
        request_mock.assert_called_with('/ws/v1/cluster/info')

    def test_cluster_metrics(self, request_mock):
        self.rm.cluster_metrics()
        request_mock.assert_called_with('/ws/v1/cluster/metrics')

    def test_cluster_scheduler(self, request_mock):
        self.rm.cluster_scheduler()
        request_mock.assert_called_with('/ws/v1/cluster/scheduler')

    def test_cluster_applications(self, request_mock):
        self.rm.cluster_applications()
        request_mock.assert_called_with('/ws/v1/cluster/apps', params={})

        self.rm.cluster_applications(state='KILLED', final_status='FAILED',
                                     user='root', queue='low', limit=10,
                                     started_time_begin=1, started_time_end=2,
                                     finished_time_begin=3, finished_time_end=4)
        request_mock.assert_called_with('/ws/v1/cluster/apps', params={'state': 'KILLED',
                                        'finalStatus': 'FAILED', 'user': 'root', 'queue': 'low',
                                        'limit': 10, 'startedTimeBegin': 1, 'startedTimeEnd': 2,
                                        'finishedTimeBegin': 3, 'finishedTimeEnd': 4})

        with self.assertRaises(IllegalArgumentError):
            self.rm.cluster_applications(state='ololo')

        with self.assertRaises(IllegalArgumentError):
            self.rm.cluster_applications(final_status='ololo')

    def test_cluster_application_statistics(self, request_mock):
        self.rm.cluster_application_statistics()
        request_mock.assert_called_with('/ws/v1/cluster/appstatistics', params={})
        # TODO: test arguments

    def test_cluster_application(self, request_mock):
        self.rm.cluster_application('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1')

    def test_cluster_application_attempts(self, request_mock):
        self.rm.cluster_application_attempts('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/appattempts')

    def test_cluster_nodes(self, request_mock):
        self.rm.cluster_nodes()
        request_mock.assert_called_with('/ws/v1/cluster/nodes', params={})

        self.rm.cluster_nodes(state='NEW', healthy='true')
        request_mock.assert_called_with('/ws/v1/cluster/nodes',
                                        params={"state": 'NEW', "healthy": 'true'})

        with self.assertRaises(IllegalArgumentError):
            self.rm.cluster_nodes(state='NEW', healthy='ololo')

    def test_cluster_node(self, request_mock):
        self.rm.cluster_node('node_1')
        request_mock.assert_called_with('/ws/v1/cluster/nodes/node_1')

    # TODO
    # def test_cluster_submit_application(self, request_mock):
    #     self.rm.cluster_submit_application()
    #     request_mock.assert_called_with('/ws/v1/cluster/apps')

    def test_cluster_new_application(self, request_mock):
        self.rm.cluster_new_application()
        request_mock.assert_called_with('/ws/v1/cluster/apps/new-application', 'POST')

    def test_cluster_get_application_queue(self, request_mock):
        self.rm.cluster_get_application_queue('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/queue')

    def test_cluster_change_application_queue(self, request_mock):
        self.rm.cluster_change_application_queue('app_1', 'queue_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/queue', 'PUT', data={"queue": 'queue_1'})

    def test_cluster_get_application_priority(self, request_mock):
        self.rm.cluster_get_application_priority('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/priority')

    def test_cluster_change_application_priority(self, request_mock):
        self.rm.cluster_change_application_priority('app_1', 'priority_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/priority', 'PUT', data={"priority": 'priority_1'})
