# -*- coding: utf-8 -*-
from mock import patch
from . import TestCase

from yarn_api_client.node_manager import NodeManager
from yarn_api_client.errors import IllegalArgumentError


@patch('yarn_api_client.node_manager.NodeManager.request')
class NodeManagerTestCase(TestCase):
    def setUp(self):
        self.nm = NodeManager('localhost')

    def test_node_information(self, request_mock):
        self.nm.node_information()
        request_mock.assert_called_with('/ws/v1/node/info')

    def test_node_applications(self, request_mock):
        self.nm.node_applications('RUNNING', 'root')
        request_mock.assert_called_with('/ws/v1/node/apps',
                                        state='RUNNING', user='root')

        self.nm.node_applications()
        request_mock.assert_called_with('/ws/v1/node/apps')

        with self.assertRaises(IllegalArgumentError):
            self.nm.node_applications('ololo', 'root')

    def test_node_application(self, request_mock):
        self.nm.node_application('app_1')
        request_mock.assert_called_with('/ws/v1/node/apps/app_1')

    def test_node_containers(self, request_mock):
        self.nm.node_containers()
        request_mock.assert_called_with('/ws/v1/node/containers')

    def test_node_container(self, request_mock):
        self.nm.node_container('container_1')
        request_mock.assert_called_with('/ws/v1/node/containers/container_1')
