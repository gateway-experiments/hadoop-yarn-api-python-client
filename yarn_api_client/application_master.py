# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .base import BaseYarnAPI
from .hadoop_conf import get_webproxy_host_port


class ApplicationMaster(BaseYarnAPI):
    """
    The MapReduce Application Master REST API's allow the user to get status
    on the running MapReduce application master. Currently this is the
    equivalent to a running MapReduce job. The information includes the jobs
    the app master is running and all the job particulars like tasks,
    counters, configuration, attempts, etc.
    """
    def __init__(self, address=None, port=8088, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout
        if address is None:
            self.logger.debug('Get configuration from hadoop conf dir')
            address, port = get_webproxy_host_port()
            self.address, self.port = address, port

    def application_information(self, application_id):
        """
        The MapReduce application master information resource provides overall
        information about that mapreduce application master.
        This includes application id, time it was started, user, name, etc.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/info'.format(
            appid=application_id)

        return self.request(path)

    def jobs(self, application_id):
        """
        The jobs resource provides a list of the jobs running on this
        application master.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs'.format(
            appid=application_id)

        return self.request(path)

    def job(self, application_id, job_id):
        """
        A job resource contains information about a particular job that was
        started by this application master. Certain fields are only accessible
        if user has permissions - depends on acl settings.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_attempts(self, job_id):
        """
        With the job attempts API, you can obtain a collection of resources
        that represent the job attempts.
        """
        pass

    def job_counters(self, application_id, job_id):
        """
        With the job counters API, you can object a collection of resources
        that represent all the counters for that job.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/counters'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_conf(self, application_id, job_id):
        """
        A job configuration resource contains information about the job
        configuration for this job.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/conf'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_tasks(self, application_id, job_id):
        """
        With the tasks API, you can obtain a collection of resources that
        represent all the tasks for a job.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks'.format(
            appid=application_id, jobid=job_id)

        return self.request(path)

    def job_task(self, application_id, job_id, task_id):
        """
        A Task resource contains information about a particular
        task within a job.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}'.format(
            appid=application_id, jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_counters(self, application_id, job_id, task_id):
        """
        With the task counters API, you can object a collection of resources
        that represent all the counters for that task.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters'.format(
            appid=application_id, jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempts(self, application_id, job_id, task_id):
        """
        With the task attempts API, you can obtain a collection of resources
        that represent a task attempt within a job.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts'.format(
            appid=application_id, jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempt(self, application_id, job_id, task_id, attempt_id):
        """
        A Task Attempt resource contains information about a particular task
        attempt within a job.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}'.format(
            appid=application_id, jobid=job_id, taskid=task_id,
            attemptid=attempt_id)

        return self.request(path)

    def task_attempt_counters(self, application_id, job_id, task_id, attempt_id):
        """
        With the task attempt counters API, you can object a collection
        of resources that represent al the counters for that task attempt.
        """
        path = '/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}/counters'.format(
            appid=application_id, jobid=job_id, taskid=task_id,
            attemptid=attempt_id)

        return self.request(path)
