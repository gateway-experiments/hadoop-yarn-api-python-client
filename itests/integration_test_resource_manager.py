# -*- coding: utf-8 -*-

import os

from pprint import pprint
from unittest import TestCase
from yarn_api_client.resource_manager import ResourceManager

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


class ResourceManagerTestCase(TestCase):
    """
    Integration test that, given a provided YARN ENDPOINT,
    execute some real scenario test against that server.

    Note that, if no YARN ENDPOINT is provided, the tests
    are ignored.
    """
    @classmethod
    def setUpClass(self):
        self.configured = False
        if os.getenv('YARN_ENDPOINT'):
            yarn_endpoint = os.getenv('YARN_ENDPOINT')
            yarn_endpoint_uri = urlparse(yarn_endpoint)

            if yarn_endpoint_uri.hostname and yarn_endpoint_uri.port:
                self.configured = True
                self.resource_manager = ResourceManager([yarn_endpoint_uri.hostname + ":" +
                                                         str(yarn_endpoint_uri.port)])

    def test_cluster_information(self):
        if self.configured:
            info = self.resource_manager.cluster_information()
            pprint(info.data)
            self.assertEqual(info.data['clusterInfo']['state'], 'STARTED')

    def test_cluster_metrics(self):
        if self.configured:
            metrics = self.resource_manager.cluster_metrics()
            pprint(metrics.data)
            self.assertGreater(metrics.data['clusterMetrics']['activeNodes'], 0)
            self.assertIsNotNone(metrics.data['clusterMetrics']['totalNodes'])

    def test_cluster_scheduler(self):
        if self.configured:
            scheduler = self.resource_manager.cluster_scheduler()
            pprint(scheduler.data)
            self.assertIsNotNone(scheduler.data['scheduler']['schedulerInfo'])

    def test_cluster_applications(self):
        if self.configured:
            apps = self.resource_manager.cluster_applications()
            pprint(apps.data)
            self.assertIsNotNone(apps.data['apps'])

    def test_cluster_application_state(self):
        if self.configured:
            apps = self.resource_manager.cluster_applications()
            appid = apps.data['apps']['app'][0]['id']
            print(appid)
            response = self.resource_manager.cluster_application_state(appid)
            pprint(response.data)
            pprint(response.data['state'])
            self.assertIsNotNone(apps.data['apps'])

    def test_cluster_application_statistics(self):
        if self.configured:
            appstats = self.resource_manager.cluster_application_statistics()
            pprint(appstats.data)
            self.assertIsNotNone(appstats.data['appStatInfo'])

    def test_cluster_nodes(self):
        if self.configured:
            nodes = self.resource_manager.cluster_nodes()
            pprint(nodes.data)
            self.assertIsNotNone(nodes.data['nodes'])

            running_nodes = self.resource_manager.cluster_nodes(state='RUNNING', healthy='true')
            pprint(running_nodes.data)
            self.assertIsNotNone(nodes.data['nodes'])
