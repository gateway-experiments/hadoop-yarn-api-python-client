# -*- coding: utf-8 -*-
from tempfile import NamedTemporaryFile

import mock
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
            with patch('yarn_api_client.hadoop_conf.get_rm_ids') as get_rm_ids_mock:
                parse_mock.return_value = 'example.com:8022'
                get_rm_ids_mock.return_value = None

                host_port = hadoop_conf.get_resource_manager_host_port()

                self.assertEqual(('example.com', '8022'), host_port)
                parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml',
                                              'yarn.resourcemanager.webapp.address')

                parse_mock.reset_mock()
                parse_mock.return_value = None

                host_port = hadoop_conf.get_resource_manager_host_port()
                self.assertIsNone(host_port)


    @mock.patch('yarn_api_client.hadoop_conf.get_rm_ids')
    @mock.patch('yarn_api_client.hadoop_conf.parse')
    @mock.patch('yarn_api_client.hadoop_conf.check_is_active_rm')
    def test_get_resource_host_port_with_ha(self, check_is_active_rm_mock, parse_mock, get_rm_ids_mock):
        get_rm_ids_mock.return_value = ['rm1', 'rm2']
        parse_mock.return_value = 'example.com:8022'
        check_is_active_rm_mock.return_value = True
        host_port = hadoop_conf.get_resource_manager_host_port()

        self.assertEqual(('example.com', '8022'), host_port)
        parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml',
                'yarn.resourcemanager.webapp.address.rm1')

        parse_mock.reset_mock()
        parse_mock.return_value = None

        host_port = hadoop_conf.get_resource_manager_host_port()
        self.assertIsNone(host_port)

    def test_get_rm_ids(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'rm1,rm2'
            rm_list = hadoop_conf.get_rm_ids(hadoop_conf.CONF_DIR)
            self.assertEqual(['rm1', 'rm2'], rm_list)
            parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml', 'yarn.resourcemanager.ha.rm-ids')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            rm_list = hadoop_conf.get_rm_ids(hadoop_conf.CONF_DIR)
            self.assertIsNone(rm_list)


    def test_get_resource_manager(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            host_port = hadoop_conf.get_resource_manager(hadoop_conf.CONF_DIR, None)

            self.assertEqual(('example.com', '8022'), host_port)
            parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml',
                    'yarn.resourcemanager.webapp.address')

            host_port = hadoop_conf.get_resource_manager(hadoop_conf.CONF_DIR, 'rm1')

            self.assertEqual(('example.com', '8022'), host_port)
            parse_mock.assert_called_with('/etc/hadoop/conf/yarn-site.xml',
                    'yarn.resourcemanager.webapp.address.rm1')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            host_port = hadoop_conf.get_resource_manager(hadoop_conf.CONF_DIR, 'rm1')
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
