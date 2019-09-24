# -*- coding: utf-8 -*-
from tempfile import NamedTemporaryFile

import mock
from mock import patch
from tests import TestCase

from yarn_api_client import hadoop_conf
import platform
import os

_http_request_method = ''
_http_getresponse_method = ''

try:
    from httplib import HTTPConnection, OK, NOT_FOUND # NOQA
    _http_request_method = 'httplib.HTTPConnection.request'
    _http_getresponse_method = 'httplib.HTTPConnection.getresponse'
except ImportError:
    from http.client import HTTPConnection, OK, NOT_FOUND # NOQA
    _http_request_method = 'http.client.HTTPConnection.request'
    _http_getresponse_method = 'http.client.HTTPConnection.getresponse'

if platform.system() == 'Windows':
    hadoop_conf_path = '/etc/hadoop/conf\\'
else:
    hadoop_conf_path = '/etc/hadoop/conf/'

empty_config = '<configuration></configuration>'.encode('latin1')

yarn_site_xml = """\
<configuration>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>localhost:8022</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.https.address</name>
    <value>localhost:8024</value>
  </property>
  <property>
    <name>yarn.http.policy</name>
    <value>HTTPS_ONLY</value>
  </property>
</configuration>
""".encode('latin1')


class HadoopConfTestCase(TestCase):
    def test_parse(self):
        temp_filename = None

        with NamedTemporaryFile(delete=False) as f:
            f.write(yarn_site_xml)
            f.flush()
            f.close()
            temp_filename = f.name

            key = 'yarn.resourcemanager.webapp.address'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual('localhost:8022', value)

            key = 'yarn.resourcemanager.webapp.https.address'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual('localhost:8024', value)

            key = 'yarn.http.policy'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual('HTTPS_ONLY', value)
        os.remove(temp_filename)

        with NamedTemporaryFile(delete=False) as f:
            f.write(empty_config)
            f.flush()
            f.close()
            temp_filename = f.name

            key = 'yarn.resourcemanager.webapp.address'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual(None, value)

            key = 'yarn.resourcemanager.webapp.https.address'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual(None, value)

            key = 'yarn.http.policy'
            value = hadoop_conf.parse(f.name, key)
            self.assertEqual(None, value)
        os.remove(temp_filename)

    def test_get_resource_endpoint(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            with patch('yarn_api_client.hadoop_conf._get_rm_ids') as get_rm_ids_mock:
                parse_mock.return_value = 'example.com:8022'
                get_rm_ids_mock.return_value = None

                endpoint = hadoop_conf.get_resource_manager_endpoint()

                self.assertEqual('example.com:8022', endpoint)
                parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml',
                                              'yarn.resourcemanager.webapp.address')

                parse_mock.reset_mock()
                parse_mock.return_value = None

                endpoint = hadoop_conf.get_resource_manager_endpoint()
                self.assertIsNone(endpoint)

    @mock.patch('yarn_api_client.hadoop_conf._get_rm_ids')
    @mock.patch('yarn_api_client.hadoop_conf.parse')
    @mock.patch('yarn_api_client.hadoop_conf.check_is_active_rm')
    def test_get_resource_endpoint_with_ha(self, check_is_active_rm_mock, parse_mock, get_rm_ids_mock):
        get_rm_ids_mock.return_value = ['rm1', 'rm2']
        parse_mock.return_value = 'example.com:8022'
        check_is_active_rm_mock.return_value = True
        endpoint = hadoop_conf.get_resource_manager_endpoint()

        self.assertEqual('example.com:8022', endpoint)
        parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml',
                                      'yarn.resourcemanager.webapp.address.rm1')

        parse_mock.reset_mock()
        parse_mock.return_value = None

        endpoint = hadoop_conf.get_resource_manager_endpoint()
        self.assertIsNone(endpoint)

    def test_get_rm_ids(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'rm1,rm2'
            rm_list = hadoop_conf._get_rm_ids(hadoop_conf.CONF_DIR)
            self.assertEqual(['rm1', 'rm2'], rm_list)
            parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml', 'yarn.resourcemanager.ha.rm-ids')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            rm_list = hadoop_conf._get_rm_ids(hadoop_conf.CONF_DIR)
            self.assertIsNone(rm_list)

    @mock.patch('yarn_api_client.hadoop_conf._is_https_only')
    @mock.patch(_http_request_method)
    @mock.patch(_http_getresponse_method)
    def test_check_is_active_rm(self, http_getresponse_mock, http_conn_request_mock, is_https_only_mock):
        class ResponseMock():
            def __init__(self, status, header_dict):
                self.status = status
                self.header_dict = header_dict

            def getheader(self, header_key, default_return):
                if header_key in self.header_dict:
                    return self.header_dict[header_key]
                else:
                    return default_return

        is_https_only_mock.return_value = False
        http_conn_request_mock.return_value = None
        http_getresponse_mock.return_value = ResponseMock(OK, {})
        self.assertTrue(hadoop_conf.check_is_active_rm('example2:8022'))
        http_getresponse_mock.reset_mock()
        http_getresponse_mock.return_value = ResponseMock(OK, {'Refresh': "testing"})
        self.assertFalse(hadoop_conf.check_is_active_rm('example2:8022'))
        http_getresponse_mock.reset_mock()
        http_getresponse_mock.return_value = ResponseMock(NOT_FOUND, {'Refresh': "testing"})
        self.assertFalse(hadoop_conf.check_is_active_rm('example2:8022'))
        http_conn_request_mock.side_effect = Exception('error')
        http_conn_request_mock.reset_mock()
        http_conn_request_mock.return_value = None
        self.assertFalse(hadoop_conf.check_is_active_rm('example2:8022'))

    def test_get_resource_manager(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            endpoint = hadoop_conf._get_resource_manager(hadoop_conf.CONF_DIR, None)

            self.assertEqual('example.com:8022', endpoint)
            parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml', 'yarn.resourcemanager.webapp.address')

            endpoint = hadoop_conf._get_resource_manager(hadoop_conf.CONF_DIR, 'rm1')

            self.assertEqual(('example.com:8022'), endpoint)
            parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml', 'yarn.resourcemanager.webapp.address.rm1')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            endpoint = hadoop_conf._get_resource_manager(hadoop_conf.CONF_DIR, 'rm1')
            self.assertIsNone(endpoint)

    def test_get_jobhistory_endpoint(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            endpoint = hadoop_conf.get_jobhistory_endpoint()

            self.assertEqual('example.com:8022', endpoint)
            parse_mock.assert_called_with(hadoop_conf_path + 'mapred-site.xml',
                                          'mapreduce.jobhistory.webapp.address')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            endpoint = hadoop_conf.get_jobhistory_endpoint()
            self.assertIsNone(endpoint)

    def test_get_nodemanager_endpoint(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            endpoint = hadoop_conf.get_nodemanager_endpoint()

            self.assertEqual('example.com:8022', endpoint)
            parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml',
                                          'yarn.nodemanager.webapp.address')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            endpoint = hadoop_conf.get_nodemanager_endpoint()
            self.assertIsNone(endpoint)

    def test_get_webproxy_endpoint(self):
        with patch('yarn_api_client.hadoop_conf.parse') as parse_mock:
            parse_mock.return_value = 'example.com:8022'

            endpoint = hadoop_conf.get_webproxy_endpoint()

            self.assertEqual('example.com:8022', endpoint)
            parse_mock.assert_called_with(hadoop_conf_path + 'yarn-site.xml',
                                          'yarn.web-proxy.address')

            parse_mock.reset_mock()
            parse_mock.return_value = None

            endpoint = hadoop_conf.get_webproxy_endpoint()
            self.assertIsNone(endpoint)
