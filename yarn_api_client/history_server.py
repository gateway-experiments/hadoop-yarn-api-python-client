# -*- coding: utf-8 -*-
from .base import BaseYarnAPI
from .hadoop_conf import get_jobhistory_host_port


class HistoryServer(BaseYarnAPI):
    def __init__(self, address=None, port=19888, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout
        if address is None:
            self.logger.debug(u'Get information from hadoop conf dir')
            address, port = get_jobhistory_host_port()
            self.address, self.port = address, port

    def application_information(self):
        path = '/ws/v1/history/info'

        return self.request(path)

    def jobs(self, state=None, user=None, queue=None, limit=None,
             started_time_begin=None, started_time_end=None,
             finished_time_begin=None, finished_time_end=None):
        path = '/ws/v1/history/mapreduce/jobs'

        loc_args = (
            ('state', state),
            ('user', user),
            ('queue', queue),
            ('limit', limit),
            ('startedTimeBegin', started_time_begin),
            ('startedTimeEnd', started_time_end),
            ('finishedTimeBegin', finished_time_begin),
            ('finishedTimeEnd', finished_time_end))

        params = {key: value for key, value in loc_args if value is not None}

        return self.request(path, **params)

    def job(self, job_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}'.format(jobid=job_id)

        return self.request(path)

    def job_attempts(self, job_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/jobattempts'.format(
            jobid=job_id)

        return self.request(path)

    def job_counters(self, job_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/counters'.format(
            jobid=job_id)

        return self.request(path)

    def job_conf(self, job_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/conf'.format(jobid=job_id)

        return self.request(path)

    def job_tasks(self, job_id, type=None):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks'.format(
            jobid=job_id)

        params = {}
        if types is not None:
            params['types'] = types

        return self.request(path, **params)

    def job_task(self, job_id, task_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_counters(self, job_id, task_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempts(self, job_id, task_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempt(self, job_id, task_id, attempt_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}'.format(
            jobid=job_id, taskid=task_id, attemptid=attempt_id)

        return self.request(path)

    def task_attempt_counters(self, job_id, task_id, attempt_id):
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}/counters'.format(
            jobid=job_id, taskid=task_id, attemptid=attempt_id)

        return self.request(path)
