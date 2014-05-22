# -*- coding: utf-8 -*-
from .base import BaseYarnAPI
from .hadoop_conf import get_webproxy_host_port


class ApplicationMaster(BaseYarnAPI):
    def __init__(self, address=None, port=8088, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout
        if address is None:
            self.logger.debug(u'Get configuration from hadoop conf dir')
            address, port = get_webproxy_host_port()
            self.address, self.port = address, port

    def application_information(self, application_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/info'.format(
            appid=application_id)

        return self.request(path)

    def jobs(self, application_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs'.format(
            appid=application_id)

        return self.request(path)

    def job(self, application_id, job_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_attempts(self, job_id):
        pass

    def job_counters(self, application_id, job_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/counters'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_conf(self, application_id, job_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/conf'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_tasks(self, application_id, job_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_task(self, application_id, job_id, task_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}'.format(
            appid=application_id, jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_counters(self, application_id, job_id, task_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters'.format(
            appid=application_id, jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempts(self, application_id, job_id, task_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts'.format(
            appid=application_id, jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempt(self, application_id, job_id, task_id, attempt_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}'.format(
            appid=application_id, jobid=job_id, taskid=task_id,
            attemptid=attempt_id)

        return self.request(path)

    def task_attempt_counters(self, application_id, job_id, task_id, attempt_id):
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}/counters'.format(
            appid=application_id, jobid=job_id, taskid=task_id,
            attemptid=attempt_id)

        return self.request(path)

