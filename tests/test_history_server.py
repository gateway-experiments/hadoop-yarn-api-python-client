# -*- coding: utf-8 -*-
from mock import patch
from tests import TestCase

from yarn_api_client.history_server import HistoryServer
from yarn_api_client.errors import IllegalArgumentError


@patch('yarn_api_client.history_server.HistoryServer.request')
class HistoryServerTestCase(TestCase):
    def setUp(self):
        self.hs = HistoryServer('localhost')

    @patch('yarn_api_client.history_server.get_jobhistory_host_port')
    def test__init__(self, get_config_mock, request_mock):
        get_config_mock.return_value = (None, None)
        HistoryServer()
        get_config_mock.assert_called_with()

    def test_application_information(self, request_mock):
        self.hs.application_information()
        request_mock.assert_called_with('/ws/v1/history/info')

    def test_jobs(self, request_mock):
        self.hs.jobs()
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs')

        self.hs.jobs(state='NEW', user='root', queue='high', limit=100,
                     started_time_begin=1, started_time_end=2,
                     finished_time_begin=3, finished_time_end=4)

        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs',
                                        queue='high',
                                        state='NEW', user='root', limit=100,
                                        startedTimeBegin=1, startedTimeEnd=2,
                                        finishedTimeBegin=3, finishedTimeEnd=4)

        with self.assertRaises(IllegalArgumentError):
            self.hs.jobs(state='ololo')

    def test_job(self, request_mock):
        self.hs.job('job_100500')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_100500')

    def test_job_attempts(self, request_mock):
        self.hs.job_attempts('job_1')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_1/jobattempts')

    def test_job_counters(self, request_mock):
        self.hs.job_counters('job_2')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/counters')

    def test_job_conf(self, request_mock):
        self.hs.job_conf('job_2')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/conf')

    def test_job_tasks(self, request_mock):
        self.hs.job_tasks('job_2')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks')
        self.hs.job_tasks('job_2', type='m')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks', types='m')

        with self.assertRaises(IllegalArgumentError):
            self.hs.job_tasks('job_2', type='ololo')

    def test_job_task(self, request_mock):
        self.hs.job_task('job_2', 'task_3')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks/task_3')

    def test_task_counters(self, request_mock):
        self.hs.task_counters('job_2', 'task_3')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks/task_3/counters')

    def test_task_attempts(self, request_mock):
        self.hs.task_attempts('job_2', 'task_3')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks/task_3/attempts')

    def test_task_attempt(self, request_mock):
        self.hs.task_attempt('job_2', 'task_3', 'attempt_4')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks/task_3/attempt/attempt_4')

    def test_task_attempt_counters(self, request_mock):
        self.hs.task_attempt_counters('job_2', 'task_3', 'attempt_4')
        request_mock.assert_called_with('/ws/v1/history/mapreduce/jobs/job_2/tasks/task_3/attempt/attempt_4/counters')
