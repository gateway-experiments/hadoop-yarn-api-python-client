# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .base import BaseYarnAPI
from .constants import JobStateInternal
from .errors import IllegalArgumentError
from .hadoop_conf import get_jobhistory_endpoint


class HistoryServer(BaseYarnAPI):
    """
    The history server REST API's allow the user to get status on finished
    applications. Currently it only supports MapReduce and provides
    information on finished jobs.

    If `service_endpoint` argument is `None` client will try to extract it from
    Hadoop configuration files.

    :param str service_endpoint: HistoryServer HTTP(S) address
    :param int timeout: API connection timeout in seconds
    :param AuthBase auth: Auth to use for requests
    :param boolean verify: Either a boolean, in which case it controls whether
        we verify the server's TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``
    """
    def __init__(self, service_endpoint=None, timeout=30, auth=None, verify=True):
        if not service_endpoint:
            self.logger.debug('Get information from hadoop conf dir')
            service_endpoint = get_jobhistory_endpoint()

        super(HistoryServer, self).__init__(service_endpoint, timeout, auth, verify)

    def application_information(self):
        """
        The history server information resource provides overall information
        about the history server.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/info'

        return self.request(path)

    def jobs(self, state=None, user=None, queue=None, limit=None,
             started_time_begin=None, started_time_end=None,
             finished_time_begin=None, finished_time_end=None):
        """
        The jobs resource provides a list of the MapReduce jobs that have
        finished. It does not currently return a full list of parameters.

        :param str user: user name
        :param str state: the job state
        :param str queue: queue name
        :param str limit: total number of app objects to be returned
        :param str started_time_begin: jobs with start time beginning with
            this time, specified in ms since epoch
        :param str started_time_end: jobs with start time ending with this
            time, specified in ms since epoch
        :param str finished_time_begin: jobs with finish time beginning with
            this time, specified in ms since epoch
        :param str finished_time_end: jobs with finish time ending with this
            time, specified in ms since epoch
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        :raises yarn_api_client.errors.IllegalArgumentError: if `state`
            incorrect
        """
        path = '/ws/v1/history/mapreduce/jobs'

        legal_states = {s for s, _ in JobStateInternal}
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

        return self.request(path, params=params)

    def job(self, job_id):
        """
        A Job resource contains information about a particular job identified
        by jobid.

        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
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

        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/counters'.format(
            jobid=job_id)

        return self.request(path)

    def job_conf(self, job_id):
        """
        A job configuration resource contains information about the job
        configuration for this job.

        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/conf'.format(jobid=job_id)

        return self.request(path)

    def job_tasks(self, job_id, job_type=None):
        """
        With the tasks API, you can obtain a collection of resources that
        represent a task within a job.

        :param str job_id: The job id
        :param str type: type of task, valid values are m or r. m for map
            task or r for reduce task
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks'.format(
            jobid=job_id)

        # m - for map
        # r - for reduce
        valid_types = ['m', 'r']
        if job_type is not None and job_type not in valid_types:
            msg = 'Job type %s is illegal' % (job_type,)
            raise IllegalArgumentError(msg)

        params = {}
        if job_type is not None:
            params['type'] = job_type

        return self.request(path, params=params)

    def job_task(self, job_id, task_id):
        """
        A Task resource contains information about a particular task
        within a job.

        :param str job_id: The job id
        :param str task_id: The task id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_counters(self, job_id, task_id):
        """
        With the task counters API, you can object a collection of resources
        that represent all the counters for that task.

        :param str job_id: The job id
        :param str task_id: The task id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempts(self, job_id, task_id):
        """
        With the task attempts API, you can obtain a collection of resources
        that represent a task attempt within a job.

        :param str job_id: The job id
        :param str task_id: The task id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts'.format(
            jobid=job_id, taskid=task_id)

        return self.request(path)

    def task_attempt(self, job_id, task_id, attempt_id):
        """
        A Task Attempt resource contains information about a particular task
        attempt within a job.

        :param str job_id: The job id
        :param str task_id: The task id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}'.format(
            jobid=job_id, taskid=task_id, attemptid=attempt_id)

        return self.request(path)

    def task_attempt_counters(self, job_id, task_id, attempt_id):
        """
        With the task attempt counters API, you can object a collection of
        resources that represent al the counters for that task attempt.

        :param str job_id: The job id
        :param str task_id: The task id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters'.format(
            jobid=job_id, taskid=task_id, attemptid=attempt_id)

        return self.request(path)
