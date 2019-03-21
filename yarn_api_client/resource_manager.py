# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .base import BaseYarnAPI
from .constants import YarnApplicationState, FinalApplicationStatus
from .errors import IllegalArgumentError
from .hadoop_conf import get_resource_manager_host_port, check_is_active_rm, CONF_DIR


class ResourceManager(BaseYarnAPI):
    """
    The ResourceManager REST API's allow the user to get information about the
    cluster - status on the cluster, metrics on the cluster,
    scheduler information, information about nodes in the cluster,
    and information about applications on the cluster.

    If `address` argument is `None` client will try to extract `address` and
    `port` from Hadoop configuration files.  If both `address` and `alt_address`
    are provided, the address corresponding to the ACTIVE HA Resource Manager will
    be used.

    :param str address: ResourceManager HTTP address
    :param int port: ResourceManager HTTP port
    :param str alt_address: Alternate ResourceManager HTTP address for HA configurations
    :param int alt_port: Alternate ResourceManager HTTP port for HA configurations
    :param int timeout: API connection timeout in seconds
    :param boolean kerberos_enabled: Flag identifying is Kerberos Security has been enabled for YARN
    """
    def __init__(self, address=None, port=8088, alt_address=None, alt_port=8088, timeout=30, kerberos_enabled=False):
        if address is None:
            self.logger.debug('Get configuration from hadoop conf dir: {conf_dir}'.format(conf_dir=CONF_DIR))
            address, port = get_resource_manager_host_port()
        else:
            if alt_address:  # Determine active RM
                if not check_is_active_rm(address, port):
                    # Default is not active, check alternate
                    if check_is_active_rm(alt_address, alt_port):
                        address, port = alt_address, alt_port

        super(ResourceManager, self).__init__(address, port, timeout, kerberos_enabled)

    def get_active_host_port(self):
        """
        The active address, port tuple to which this instance is associated.

        :return: Tuple (str, int) corresponding to the active address and port
        """
        return self.address, self.port

    def cluster_information(self):
        """
        The cluster information resource provides overall information about
        the cluster.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/info'
        return self.request(path)

    def cluster_metrics(self):
        """
        The cluster metrics resource provides some overall metrics about the
        cluster. More detailed metrics should be retrieved from the jmx
        interface.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/metrics'
        return self.request(path)

    def cluster_scheduler(self):
        """
        A scheduler resource contains information about the current scheduler
        configured in a cluster. It currently supports both the Fifo and
        Capacity Scheduler. You will get different information depending on
        which scheduler is configured so be sure to look at the type
        information.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/scheduler'
        return self.request(path)

    def cluster_applications(self, state=None, final_status=None,
                             user=None, queue=None, limit=None,
                             started_time_begin=None, started_time_end=None,
                             finished_time_begin=None, finished_time_end=None):
        """
        With the Applications API, you can obtain a collection of resources,
        each of which represents an application.

        :param str state: state of the application
        :param str final_status: the final status of the
            application - reported by the application itself
        :param str user: user name
        :param str queue: queue name
        :param str limit: total number of app objects to be returned
        :param str started_time_begin: applications with start time beginning
            with this time, specified in ms since epoch
        :param str started_time_end: applications with start time ending with
            this time, specified in ms since epoch
        :param str finished_time_begin: applications with finish time
            beginning with this time, specified in ms since epoch
        :param str finished_time_end: applications with finish time ending
            with this time, specified in ms since epoch
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        :raises yarn_api_client.errors.IllegalArgumentError: if `state` or
            `final_status` incorrect
        """
        path = '/ws/v1/cluster/apps'

        legal_states = {s for s, _ in YarnApplicationState}
        if state is not None and state not in legal_states:
            msg = 'Yarn Application State %s is illegal' % (state,)
            raise IllegalArgumentError(msg)

        legal_final_statuses = {s for s, _ in FinalApplicationStatus}
        if final_status is not None and final_status not in legal_final_statuses:
            msg = 'Final Application Status %s is illegal' % (final_status,)
            raise IllegalArgumentError(msg)

        loc_args = (
            ('state', state),
            ('finalStatus', final_status),
            ('user', user),
            ('queue', queue),
            ('limit', limit),
            ('startedTimeBegin', started_time_begin),
            ('startedTimeEnd', started_time_end),
            ('finishedTimeBegin', finished_time_begin),
            ('finishedTimeEnd', finished_time_end))

        params = self.construct_parameters(loc_args)

        return self.request(path, **params)

    def cluster_application_statistics(self, state_list=None,
                                       application_type_list=None):
        """
        With the Application Statistics API, you can obtain a collection of
        triples, each of which contains the application type, the application
        state and the number of applications of this type and this state in
        ResourceManager context.

        This method work in Hadoop > 2.0.0

        :param list state_list: states of the applications, specified as a
            comma-separated list. If states is not provided, the API will
            enumerate all application states and return the counts of them.
        :param list application_type_list: types of the applications,
            specified as a comma-separated list. If application_types is not
            provided, the API will count the applications of any application
            type. In this case, the response shows * to indicate any
            application type. Note that we only support at most one
            applicationType temporarily. Otherwise, users will expect
            an BadRequestException.
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/appstatistics'

        # TODO: validate state argument
        states = ','.join(state_list) if state_list is not None else None
        if application_type_list is not None:
            application_types = ','.join(application_type_list)
        else:
            application_types = None

        loc_args = (
            ('states', states),
            ('applicationTypes', application_types))
        params = self.construct_parameters(loc_args)

        return self.request(path, **params)

    def cluster_application(self, application_id):
        """
        An application resource contains information about a particular
        application that was submitted to a cluster.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}'.format(appid=application_id)

        return self.request(path)

    def cluster_application_attempts(self, application_id):
        """
        With the application attempts API, you can obtain a collection of
        resources that represent an application attempt.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/appattempts'.format(
            appid=application_id)

        return self.request(path)

    def cluster_application_attempt_info(self, application_id, attempt_id):
        """
        With the application attempts API, you can obtain an extended info about
        an application attempt.

        :param str application_id: The application id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/appattempts/{attemptid}'.format(
            appid=application_id, attemptid=attempt_id)

        return self.request(path)

    def cluster_application_attempt_containers(self, application_id, attempt_id):
        """
        With the application attempts API, you can obtain an information
        about container related to an application attempt.

        :param str application_id: The application id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/appattempts/{attemptid}/containers'.format(
            appid=application_id, attemptid=attempt_id)

        return self.request(path)

    def cluster_application_state(self, application_id):
        """
        With the application state API, you can obtain the current
        state of an application.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/state'.format(
            appid=application_id)

        return self.request(path)

    def cluster_application_kill(self, application_id):
        """
        With the application kill API, you can kill an application
        that is not in FINISHED or FAILED state.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """

        data = '{"state": "KILLED"}'
        path = '/ws/v1/cluster/apps/{appid}/state'.format(
            appid=application_id)

        return self.request(path, 'PUT', data=data)

    def cluster_nodes(self, state=None, healthy=None):
        """
        With the Nodes API, you can obtain a collection of resources, each of
        which represents a node.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        :raises yarn_api_client.errors.IllegalArgumentError: if `healthy`
            incorrect
        """
        path = '/ws/v1/cluster/nodes'
        # TODO: validate state argument

        legal_healthy = ['true', 'false']
        if healthy is not None and healthy not in legal_healthy:
            msg = 'Valid Healthy arguments are true, false'
            raise IllegalArgumentError(msg)

        loc_args = (
            ('state', state),
            ('healthy', healthy),
        )
        params = self.construct_parameters(loc_args)

        return self.request(path, params=params)

    def cluster_node(self, node_id):
        """
        A node resource contains information about a node in the cluster.

        :param str node_id: The node id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/nodes/{nodeid}'.format(nodeid=node_id)

        return self.request(path)

    def cluster_submit_application(self, data):
        """
        With the New Application API, you can obtain an application-id which
        can then be used as part of the Cluster Submit Applications API to
        submit applications. The response also includes the maximum resource
        capabilities available on the cluster.

        For data body definition refer to:
        (https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Writeable_APIs)

        :param dict data: Application details
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps'

        return self.request(path, 'POST', data=data)

    def cluster_new_application(self):
        """
        * This feature is currently in the alpha stage and may change in the
        future *

        With the New Application API, you can obtain an application-id which
        can then be used as part of the Cluster Submit Applications API to
        submit applications. The response also includes the maximum resource
        capabilities available on the cluster.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/new-application'

        return self.request(path, 'POST')

    def cluster_get_application_queue(self, application_id):
        """
        * This feature is currently in the alpha stage and may change in the
        future *

        With the application queue API, you can query the queue of a
        submitted app

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/queue'.format(appid=application_id)

        return self.request(path)

    def cluster_change_application_queue(self, application_id, queue):
        """
        * This feature is currently in the alpha stage and may change in the
        future *

        Move a running app to another queue using a PUT request specifying the
        target queue.

        To perform the PUT operation, authentication has to be
        setup for the RM web services. In addition, you must be authorized to
        move the app. Currently you can only move the app if you’re using the
        Capacity scheduler or the Fair scheduler.

        Please note that in order to move an app, you must have an
        authentication filter setup for the HTTP interface. The functionality
        requires that a username is set in the HttpServletRequest. If no filter
        is setup, the response will be an “UNAUTHORIZED” response.

        :param str application_id: The application id
        :param str queue: queue name
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/queue'.format(appid=application_id)

        return self.request(path, 'PUT', data={"queue": queue})

    def cluster_get_application_priority(self, application_id):
        """
        * This feature is currently in the alpha stage and may change in the
        future *

        With the application priority API, you can query the priority of a
        submitted app

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/priority'.format(appid=application_id)

        return self.request(path)

    def cluster_change_application_priority(self, application_id, priority):
        """
        * This feature is currently in the alpha stage and may change in the
        future *

        Update priority of a running or accepted app using a PUT request
        specifying the target priority.

        To perform the PUT operation, authentication has to be
        setup for the RM web services. In addition, you must be authorized to
        move the app. Currently you can only move the app if you’re using the
        Capacity scheduler or the Fair scheduler.

        Please note that in order to move an app, you must have an
        authentication filter setup for the HTTP interface. The functionality
        requires that a username is set in the HttpServletRequest. If no filter
        is setup, the response will be an “UNAUTHORIZED” response.

        :param str application_id: The application id
        :param int priority: application priority
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/priority'.format(appid=application_id)

        return self.request(path, 'PUT', data={"priority": priority})
