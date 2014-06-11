# -*- coding: utf-8 -*-
from mock import patch
from tests import TestCase

from yarn_api_client.application_master import ApplicationMaster


@patch('yarn_api_client.application_master.ApplicationMaster.request')
class AppMasterTestCase(TestCase):
    def setUp(self):
        self.app = ApplicationMaster('localhost')

    @patch('yarn_api_client.application_master.get_webproxy_host_port')
    def test__init__(self, get_config_mock, request_mock):
        get_config_mock.return_value = (None, None)
        ApplicationMaster()
        get_config_mock.assert_called_with()

    def test_application_information(self, request_mock):
        self.app.application_information('app_100500')
        request_mock.assert_called_with('/proxy/app_100500/ws/v1/mapreduce/info')

    def test_jobs(self, request_mock):
        self.app.jobs('app_100500')
        request_mock.assert_called_with('/proxy/app_100500/ws/v1/mapreduce/jobs')

    def test_job(self, request_mock):
        self.app.job('app_100500', 'job_100500')
        request_mock.assert_called_with('/proxy/app_100500/ws/v1/mapreduce/jobs/job_100500')

    def test_job_attempts(self, request_mock):
        self.app.job_attempts('app_1')

    def test_job_counters(self, request_mock):
        self.app.job_counters('app_1', 'job_2')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/counters')

    def test_job_conf(self, request_mock):
        self.app.job_conf('app_1', 'job_2')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/conf')

    def test_job_tasks(self, request_mock):
        self.app.job_tasks('app_1', 'job_2')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/tasks')

    def test_job_task(self, request_mock):
        self.app.job_task('app_1', 'job_2', 'task_3')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/tasks/task_3')

    def test_task_counters(self, request_mock):
        self.app.task_counters('app_1', 'job_2', 'task_3')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/tasks/task_3/counters')

    def test_task_attempts(self, request_mock):
        self.app.task_attempts('app_1', 'job_2', 'task_3')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/tasks/task_3/attempts')

    def test_task_attempt(self, request_mock):
        self.app.task_attempt('app_1', 'job_2', 'task_3', 'attempt_4')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/tasks/task_3/attempt/attempt_4')

    def test_task_attempt_counters(self, request_mock):
        self.app.task_attempt_counters('app_1', 'job_2', 'task_3', 'attempt_4')
        request_mock.assert_called_with('/proxy/app_1/ws/v1/mapreduce/jobs/job_2/tasks/task_3/attempt/attempt_4/counters')
