# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .base import BaseYarnAPI
from .constants import JobStateInternal
from .errors import IllegalArgumentError
from .hadoop_conf import get_jobhistory_host_port


class HistoryServer(BaseYarnAPI):
    """
    The history server REST API's allow the user to get status on finished
    applications. Currently it only supports MapReduce and provides
    information on finished jobs.
    """
    def __init__(self, address=None, port=19888, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout
        if address is None:
            self.logger.debug('Get information from hadoop conf dir')
            address, port = get_jobhistory_host_port()
            self.address, self.port = address, port

    def application_information(self):
        """
        The history server information resource provides overall information
        about the history server.
        """
        path = '/ws/v1/history/info'

        return self.request(path)

    def jobs(self, state=None, user=None, queue=None, limit=None,
             started_time_begin=None, started_time_end=None,
             finished_time_begin=None, finished_time_end=None):
        """
        The jobs resource provides a list of the MapReduce jobs that have
        finished. It does not currently return a full list of parameters.
        """
        path = '/ws/v1/history/mapreduce/jobs'

        legal_states = set([s for s, _ in JobStateInternal])
        if state is not None and state not in legal_states:
            msg = 'Job Internal State %s is illegal' % (state,)
            raise IllegalArgumentError(msg)

        loc_args = (
            ('state', state),
            ('user', user),
            ('queue', queue),
            ('limit', limit),
            ('startedTimeBegin', started_time_begin),
            ('startedTimeEnd', started_time_end),
            ('finishedTimeBegin', finished_time_begin),
            ('finishedTimeEnd', finished_time_end))

        params = self.construct_parameters(loc_args)

        return self.request(path, **params)

    def job(self, job_id):
        """
        A Job resource contains information about a particular job identified
        by jobid.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}'.format(jobid=job_id)

        return self.request(path)

    def job_attempts(self, job_id):
        """
        With the job attempts API, you can obtain a collection of resources
        that represent a job attempt.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/jobattempts'.format(
            jobid=job_id)

        return self.request(path)

    def job_counters(self, job_id):
        """
        With the job counters API, you can object a collection of resources
        that represent al the counters for that job.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/counters'.format(
            jobid=job_id)

        return self.request(path)

    def job_conf(self, job_id):
        """
        A job configuration resource contains information about the job
        configuration for this job.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/conf'.format(jobid=job_id)

        return self.request(path)

    def job_tasks(self, job_id, type=None):
        """
        With the tasks API, you can obtain a collection of resources that
        represent a task within a job.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks'.format(
            jobid=job_id)

        # m - for map
        # r - for reduce
        valid_types = ['m', 'r']
        if type is not None and type not in valid_types:
            msg = 'Job type %s is illegal' % (type,)
            raise IllegalArgumentError(msg)

        params = {}
        if type is not None:
            params['type'] = type

        return self.request(path, **params)

    def job_task(self, job_id, task_id):
        """
        A Task resource contains information about a particular task
        within a job.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_counters(self, job_id, task_id):
        """
        With the task counters API, you can object a collection of resources
        that represent all the counters for that task.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempts(self, job_id, task_id):
        """
        With the task attempts API, you can obtain a collection of resources
        that represent a task attempt within a job.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempt(self, job_id, task_id, attempt_id):
        """
        A Task Attempt resource contains information about a particular task
        attempt within a job.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}'.format(
            jobid=job_id, taskid=task_id, attemptid=attempt_id)

        return self.request(path)

    def task_attempt_counters(self, job_id, task_id, attempt_id):
        """
        With the task attempt counters API, you can object a collection of
        resources that represent al the counters for that task attempt.
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempt/{attemptid}/counters'.format(
            jobid=job_id, taskid=task_id, attemptid=attempt_id)

        return self.request(path)
