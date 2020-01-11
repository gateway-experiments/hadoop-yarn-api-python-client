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
        get_config_mock.return_value = "https://localhost"
        rm = ResourceManager()
        get_config_mock.assert_called_with(30, None, True)
        self.assertEqual(rm.service_uri.is_https, True)

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

        self.rm.cluster_applications(state='KILLED', states=['KILLED'],
                                     final_status='FAILED', user='root',
                                     queue='low', limit=10,
                                     started_time_begin=1, started_time_end=2,
                                     finished_time_begin=3, finished_time_end=4,
                                     application_types=['YARN'],
                                     application_tags=['apptag'],
                                     de_selects=['resouceRequests'])
        request_mock.assert_called_with('/ws/v1/cluster/apps', params={
            'state': 'KILLED',
            'states': 'KILLED',
            'finalStatus': 'FAILED',
            'user': 'root',
            'queue': 'low',
            'limit': 10,
            'startedTimeBegin': 1,
            'startedTimeEnd': 2,
            'finishedTimeBegin': 3,
            'finishedTimeEnd': 4,
            'applicationTypes': 'YARN',
            'applicationTags': 'apptag',
            'deSelects': 'resouceRequests'
        })

        with self.assertRaises(IllegalArgumentError):
            self.rm.cluster_applications(states=['ololo'])

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

    def test_cluster_application_attempt_info(self, request_mock):
        self.rm.cluster_application_attempt_info('app_1', 'attempt_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/appattempts/attempt_1')

    def test_cluster_application_attempt_containers(self, request_mock):
        self.rm.cluster_application_attempt_containers('app_1', 'attempt_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/appattempts/attempt_1/containers')

    def test_cluster_application_attempt_container_info(self, request_mock):
        self.rm.cluster_application_attempt_container_info('app_1', 'attempt_1', 'container_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/appattempts/attempt_1/containers/container_1')

    def test_cluster_application_state(self, request_mock):
        self.rm.cluster_application_state('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/state')

    def test_cluster_application_kill(self, request_mock):
        self.rm.cluster_application_kill('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/state', 'PUT', data={
            "state": 'KILLED'
        })

    def test_cluster_nodes(self, request_mock):
        self.rm.cluster_nodes()
        request_mock.assert_called_with('/ws/v1/cluster/nodes', params={})

        self.rm.cluster_nodes(states=['NEW'])
        request_mock.assert_called_with('/ws/v1/cluster/nodes', params={
            "states": 'NEW'
        })

        with self.assertRaises(IllegalArgumentError):
            self.rm.cluster_nodes(states=['ololo'])

    def test_cluster_node(self, request_mock):
        self.rm.cluster_node('node_1')
        request_mock.assert_called_with('/ws/v1/cluster/nodes/node_1')

    def test_cluster_submit_application(self, request_mock):
        self.rm.cluster_submit_application({"application-name": "dummy_application"})
        request_mock.assert_called_with('/ws/v1/cluster/apps', 'POST', data={
            "application-name": "dummy_application"
        })

    def test_cluster_new_application(self, request_mock):
        self.rm.cluster_new_application()
        request_mock.assert_called_with('/ws/v1/cluster/apps/new-application', 'POST')

    def test_cluster_get_application_queue(self, request_mock):
        self.rm.cluster_get_application_queue('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/queue')

    def test_cluster_change_application_queue(self, request_mock):
        self.rm.cluster_change_application_queue('app_1', 'queue_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/queue', 'PUT', data={
            "queue": 'queue_1'
        })

    def test_cluster_get_application_priority(self, request_mock):
        self.rm.cluster_get_application_priority('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/priority')

    def test_cluster_change_application_priority(self, request_mock):
        self.rm.cluster_change_application_priority('app_1', 'priority_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/priority', 'PUT', data={
            "priority": 'priority_1'
        })

    @patch('yarn_api_client.hadoop_conf.parse')
    def test_cluster_node_container_memory(self, parse_mock, request_mock):
        parse_mock.return_value = 1024
        value = self.rm.cluster_node_container_memory()
        self.assertEqual(value, 1024)

    # TODO
    # def test_cluster_scheduler_queue(self, request_mock):
    #     class ResponseMock():
    #         def __init__(self, status, data):
    #             self.status = status
    #             self.data = data

    #     request_mock.return_value = ResponseMock(
    #         'OK',
    #         {
    #             'scheduler': {
    #                 'schedulerInfo': {
    #                     "queues": {
    #                         "queue": [
    #                             {
    #                                 'queueName': 'queue_1',
    #                                 'queues': {
    #                                     'queue': [
    #                                         {
    #                                             "queueName": 'queue_2',
    #                                             'queues': {
    #                                                 'queue': [
    #                                                     {
    #                                                         'queueName': 'queue_3'
    #                                                     }
    #                                                 ]
    #                                             }
    #                                         }
    #                                     ]
    #                                 }
    #                             }
    #                         ]
    #                     }
    #                 }
    #             }
    #         }
    #     )
    #     value = self.rm.cluster_scheduler_queue('queue_1')
    #     self.assertIsNotNone(value)

    #     request_mock.return_value = ResponseMock(
    #         'OK',
    #         {
    #             'scheduler': {
    #                 'schedulerInfo': {
    #                     'queueName': 'queue_1'
    #                 }
    #             }
    #         }
    #     )
    #     value = self.rm.cluster_scheduler_queue('queue_2')
    #     self.assertIsNone(value)

    def test_cluster_scheduler_queue_availability(self, request_mock):
        value = self.rm.cluster_scheduler_queue_availability({'absoluteUsedCapacity': 90}, 70)
        self.assertEqual(value, False)

        value = self.rm.cluster_scheduler_queue_availability({'absoluteUsedCapacity': 50}, 70)
        self.assertEqual(value, True)

    def test_cluster_queue_partition(self, request_mock):
        value = self.rm.cluster_queue_partition(
            {
                'capacities': {
                    'queueCapacitiesByPartition': [
                        {
                            'partitionName': 'label_1'
                        },
                        {
                            'partitionName': 'label_2'
                        }
                    ]
                },
            },
            'label_1'
        )
        self.assertIsNotNone(value)

        value = self.rm.cluster_queue_partition(
            {
                'capacities': {
                    'queueCapacitiesByPartition': [
                        {
                            'partitionName': 'label_1'
                        },
                        {
                            'partitionName': 'label_2'
                        }
                    ]
                },
            },
            'label_3'
        )
        self.assertIsNone(value)

    def test_cluster_reservations(self, request_mock):
        self.rm.cluster_reservations('queue_1', 'reservation_1', 0, 5, True)
        request_mock.assert_called_with('/ws/v1/cluster/reservation/list', params={
            "queue": "queue_1",
            "reservation-id": "reservation_1",
            "start-time": 0,
            "end-time": 5,
            "include-resource-allocations": True
        })

    def test_cluster_new_delegation_token(self, request_mock):
        self.rm.cluster_new_delegation_token('renewer_1')
        request_mock.assert_called_with('/ws/v1/cluster/delegation-token', 'POST', data={
            "renewer": "renewer_1"
        })

    def test_cluster_renew_delegation_token(self, request_mock):
        self.rm.cluster_renew_delegation_token('delegation_token_1')
        request_mock.assert_called_with('/ws/v1/cluster/delegation-token/expiration', 'POST', headers={
            "Hadoop-YARN-RM-Delegation-Token": 'delegation_token_1'
        })

    def test_cluster_cancel_delegation_token(self, request_mock):
        self.rm.cluster_cancel_delegation_token('delegation_token_1')
        request_mock.assert_called_with('/ws/v1/cluster/delegation-token', 'DELETE', headers={
            "Hadoop-YARN-RM-Delegation-Token": 'delegation_token_1'
        })

    def test_cluster_new_reservation(self, request_mock):
        self.rm.cluster_new_reservation()
        request_mock.assert_called_with('/ws/v1/cluster/reservation/new-reservation', 'POST')

    def test_cluster_submit_reservation(self, request_mock):
        self.rm.cluster_submit_reservation({'reservation-id': 'reservation_1'})
        request_mock.assert_called_with('/ws/v1/cluster/reservation/submit', 'POST', data={
            'reservation-id': 'reservation_1'
        })

    def test_cluster_update_reservation(self, request_mock):
        self.rm.cluster_update_reservation({
            'reservation-id': 'reservation_1'
        })
        request_mock.assert_called_with('/ws/v1/cluster/reservation/update', 'POST', data={
            'reservation-id': 'reservation_1'
        })

    def test_cluster_delete_reservation(self, request_mock):
        self.rm.cluster_delete_reservation('reservation_1')
        request_mock.assert_called_with('/ws/v1/cluster/reservation/delete', 'POST', data={
            'reservation-id': 'reservation_1'
        })

    def test_cluster_application_timeouts(self, request_mock):
        self.rm.cluster_application_timeouts('app_1')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/timeouts')

    def test_cluster_application_timeout(self, request_mock):
        self.rm.cluster_application_timeout('app_1', 'LIFETIME')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/timeouts/LIFETIME')

    def test_cluster_update_application_timeout(self, request_mock):
        self.rm.cluster_update_application_timeout('app_1', 'LIFETIME', '2016-12-05T22:51:00.104+0530')
        request_mock.assert_called_with('/ws/v1/cluster/apps/app_1/timeout', 'PUT', data={
            'timeout': {'type': 'LIFETIME', 'expiryTime': '2016-12-05T22:51:00.104+0530'}
        })

    def test_cluster_scheduler_conf_mutation(self, request_mock):
        self.rm.cluster_scheduler_conf_mutation()
        request_mock.assert_called_with('/ws/v1/cluster/scheduler-conf')

    def test_cluster_modify_scheduler_conf_mutation(self, request_mock):
        self.rm.cluster_modify_scheduler_conf_mutation({
            'queue-name': 'queue_1',
            'params': {
                'test': 'test'
            }
        })
        request_mock.assert_called_with('/ws/v1/cluster/scheduler-conf', 'PUT', data={
            'queue-name': 'queue_1',
            'params': {
                'test': 'test'
            }
        })
