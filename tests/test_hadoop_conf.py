# -*- coding: utf-8 -*-
from tempfile import NamedTemporaryFile

from mock import patch
from tests import TestCase

from yarn_api_client import hadoop_conf

empty_config = '<configuration></configuration>'.encode('latin1')

yarn_site_xml = """\
<configuration>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>localhost:8022</value>
  </property>
</configuration>
""".encode('latin1')


class HadoopConfTestCase(TestCase):
    def test_parse(self):
        with NamedTemporaryFile() as f:
            f.write(yarn_site_xml)
            f.flush()

            key = 'yarn.resourcemanager.webapp.address'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual('localhost:8022', value)

        with NamedTemporaryFile() as f:
            f.write(empty_config)
            f.flush()

            key = 'yarn.resourcemanager.webapp.address'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual(None, value)

    def test_get_resource_host_port(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            host_port = hadoop_conf.get_resource_manager_host_port()

            self.assertEqual(('example.com', '8022'), host_port)
            parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml',
                                          'yarn.resourcemanager.webapp.address')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            host_port = hadoop_conf.get_resource_manager_host_port()
            self.assertIsNone(host_port)

    def test_get_jobhistory_host_port(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            host_port = hadoop_conf.get_jobhistory_host_port()

            self.assertEqual(('example.com', '8022'), host_port)
            parse_mock.assert_called_with('/etc/hadoop/conf/mapred-site.xml',
                                          'mapreduce.jobhistory.webapp.address')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            host_port = hadoop_conf.get_jobhistory_host_port()
            self.assertIsNone(host_port)

    def test_get_webproxy_host_port(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            host_port = hadoop_conf.get_webproxy_host_port()

            self.assertEqual(('example.com', '8022'), host_port)
            parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml',
                                          'yarn.web-proxy.address')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            host_port = hadoop_conf.get_webproxy_host_port()
            self.assertIsNone(host_port)
