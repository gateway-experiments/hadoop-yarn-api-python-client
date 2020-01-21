# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .base import BaseYarnAPI, get_logger
from .constants import YarnApplicationState, FinalApplicationStatus
from .errors import IllegalArgumentError
from .hadoop_conf import get_resource_manager_endpoint, check_is_active_rm, CONF_DIR, _get_maximum_container_memory
from collections import deque

log = get_logger(__name__)
LEGAL_STATES = {s for s, _ in YarnApplicationState}
LEGAL_FINAL_STATUSES = {s for s, _ in FinalApplicationStatus}


def validate_yarn_application_state(state, required=False):
    if state:
        if state not in LEGAL_STATES:
            msg = 'Yarn Application State %s is illegal' % (state,)
            raise IllegalArgumentError(msg)
    else:
        if required:
            msg = "state argument is required to be provided"
            raise IllegalArgumentError(msg)


def validate_yarn_application_states(states, required=False):
    if states:
        if not isinstance(states, list):
            msg = "States should be list"
            raise IllegalArgumentError(msg)

        illegal_states = set(states) - LEGAL_STATES
        if illegal_states:
            msg = 'Yarn Application States %s are illegal' % (
                ",".join(illegal_states),
            )
            raise IllegalArgumentError(msg)
    else:
        if required:
            msg = "states argument is required to be provided"
            raise IllegalArgumentError(msg)


def validate_final_application_status(final_status, required=False):
    if final_status:
        if final_status not in LEGAL_FINAL_STATUSES:
            msg = 'Final Application Status %s is illegal' % (final_status,)
            raise IllegalArgumentError(msg)
    else:
        if required:
            msg = "final_status argument is required to be provided"
            raise IllegalArgumentError(msg)


class ResourceManager(BaseYarnAPI):
    """
    The ResourceManager REST API's allow the user to get information about the
    cluster - status on the cluster, metrics on the cluster,
    scheduler information, information about nodes in the cluster,
    and information about applications on the cluster.

    If `service_endpoint` argument is `None` client will try to extract it from
    Hadoop configuration files.  If both `address` and `alt_address` are
    provided, the address corresponding to the ACTIVE HA Resource Manager will
    be used.

    :param List[str] service_endpoints: List of ResourceManager HTTP(S)
        addresses
    :param int timeout: API connection timeout in seconds
    :param AuthBase auth: Auth to use for requests configurations
    :param boolean verify: Either a boolean, in which case it controls whether
        we verify the server's TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``
    """
    def __init__(self, service_endpoints=None, timeout=30, auth=None, verify=True):
        active_service_endpoint = None
        if not service_endpoints:
            active_service_endpoint = get_resource_manager_endpoint(timeout, auth, verify)
        else:
            for endpoint in service_endpoints:
                if check_is_active_rm(endpoint, timeout, auth, verify):
                    active_service_endpoint = endpoint
                    break

        if active_service_endpoint:
            super(ResourceManager, self).__init__(active_service_endpoint, timeout, auth, verify)
        else:
            raise Exception("No active RMs found")

    def get_active_endpoint(self):
        """
        The active address, port tuple to which this instance is associated.
        :return: str service_endpoint: Service endpoint URL corresponding to
        the active address of RM
        """
        return self.service_uri.to_url()

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

    def cluster_applications(self, state=None, states=None,
                             final_status=None, user=None,
                             queue=None, limit=None,
                             started_time_begin=None, started_time_end=None,
                             finished_time_begin=None, finished_time_end=None,
                             application_types=None, application_tags=None,
                             de_selects=None):
        """
        With the Applications API, you can obtain a collection of resources,
        each of which represents an application.

        :param str state: state of the application [deprecated]
        :param List[str] states: applications matching the given application
            states
        :param str final_status: the final status of the application -
            reported by the application itself
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
        :param List[str] application_types: applications matching the given
            application types, specified as a comma-separated list
        :param List[str] application_tags: applications matching any of the
            given application tags, specified as a comma-separated list
        :param List[str] de_selects: a generic fields which will be skipped in
            the result
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        :raises yarn_api_client.errors.IllegalArgumentError: if `state` or
            `final_status` incorrect
        """
        path = '/ws/v1/cluster/apps'

        validate_yarn_application_state(state)
        validate_yarn_application_states(states)
        validate_final_application_status(final_status)

        loc_args = (
            ('state', state),
            ('states', ','.join(states) if states else None),
            ('finalStatus', final_status),
            ('user', user),
            ('queue', queue),
            ('limit', limit),
            ('startedTimeBegin', started_time_begin),
            ('startedTimeEnd', started_time_end),
            ('finishedTimeBegin', finished_time_begin),
            ('finishedTimeEnd', finished_time_end),
            ('applicationTypes', ','.join(application_types) if application_types else None),
            ('applicationTags', ','.join(application_tags) if application_tags else None),
            ('deSelects', ','.join(de_selects) if de_selects else None)
        )

        params = self.construct_parameters(loc_args)

        return self.request(path, params=params)

    def cluster_application_statistics(self, states=None,
                                       application_types=None):
        """
        With the Application Statistics API, you can obtain a collection of
        triples, each of which contains the application type, the application
        state and the number of applications of this type and this state in
        ResourceManager context.

        This method work in Hadoop > 2.0.0

        :param List[str] states: states of the applications. If states is not
            provided, the API will enumerate all application states and
            return the counts of them.
        :param List[str] application_types: types of the applications,
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

        validate_yarn_application_states(states)

        loc_args = (
            ('states', ','.join(states) if states else None),
            ('applicationTypes', ','.join(application_types) if application_types else None)
        )
        params = self.construct_parameters(loc_args)

        return self.request(path, params=params)

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

    def cluster_application_attempt_container_info(self, application_id, attempt_id, container_id):
        """
        With the application attempts API, you can obtain an information
        about container related to an application attempt.

        :param str application_id: The application id
        :param str attempt_id: The attempt id
        :param str container_id: The container id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/appattempts/{attemptid}/containers/{containerid}'.format(
            appid=application_id, attemptid=attempt_id, containerid=container_id)

        return self.request(path)

    def cluster_application_state(self, application_id):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

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
        (This feature is currently in the alpha stage and may change in the
        future)

        With the application kill API, you can kill an application
        that is not in FINISHED or FAILED state.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """

        data = {"state": "KILLED"}
        path = '/ws/v1/cluster/apps/{appid}/state'.format(
            appid=application_id)

        return self.request(path, 'PUT', json=data)

    def cluster_nodes(self, states=None):
        """
        With the Nodes API, you can obtain a collection of resources, each of
        which represents a node.

        :param List[str] states: the states of the node, specified as a
            comma-separated list valid values are: NEW, RUNNING, UNHEALTHY,
            DECOMMISSIONING, DECOMMISSIONED, LOST, REBOOTED, SHUTDOWN
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        :raises yarn_api_client.errors.IllegalArgumentError: if `healthy`
            incorrect
        """
        path = '/ws/v1/cluster/nodes'

        validate_yarn_application_states(states)

        loc_args = (
            ('states', ','.join(states) if states else None),
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
        (This feature is currently in the alpha stage and may change in the
        future)

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

        return self.request(path, 'POST', json=data)

    def cluster_new_application(self):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

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
        (This feature is currently in the alpha stage and may change in the
        future)

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
        (This feature is currently in the alpha stage and may change in the
        future)

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

        return self.request(path, 'PUT', json={"queue": queue})

    def cluster_get_application_priority(self, application_id):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

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
        (This feature is currently in the alpha stage and may change in the
        future)

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

        return self.request(path, 'PUT', json={"priority": priority})

    def cluster_node_container_memory(self):
        """
        This endpoint allows clients to gather info on the maximum memory that
        can be allocated per container in the cluster.
        :returns: integer specifying the maximum memory that can be allocated in
        a container in the cluster
        """

        return _get_maximum_container_memory(CONF_DIR)

    def cluster_scheduler_queue(self, yarn_queue_name):
        """
        Given a queue name, this function tries to locate the given queue in
        the object returned by scheduler endpoint.

        The queue can be present inside a multilevel structure. This solution
        tries to locate the queue using breadth-first-search algorithm.

        :param str yarn_queue_name: case sensitive queue name
        :return: queue, None if not found
        :rtype: dict
        """
        scheduler = self.cluster_scheduler().data
        scheduler_info = scheduler['scheduler']['schedulerInfo']

        bfs_deque = deque([scheduler_info])
        while bfs_deque:
            vertex = bfs_deque.popleft()
            if vertex['queueName'] == yarn_queue_name:
                return vertex
            elif 'queues' in vertex:
                for queue in vertex['queues']['queue']:
                    bfs_deque.append(queue)

        return None

    def cluster_scheduler_queue_availability(self, candidate_partition, availability_threshold):
        """
        Checks whether the requested memory satisfies the available space of the queue
        This solution takes into consideration the node label concept in cluster.
        Following node labelling, the resources can be available in various partition.
        Given the partition data it tells you if the used capacity of this partition is spilling
        the threshold specified.

        :param str candidate_parition: node label partition (case sensitive)
        :param float availability_threshold: value can range between 0 - 100 .
        :return: Boolean
        """

        if candidate_partition['absoluteUsedCapacity'] > availability_threshold:
            return False
        return True

    def cluster_queue_partition(self, candidate_queue, cluster_node_label):
        """
        A queue can be divided into multiple partitions having different node labels.
        Given the candidate queue and parition node label, this extracts the partition
        we are interested in.

        :param dict candidate_queue: queue dictionary
        :param str cluster_node_label: case sensitive node label name
        :return: partition, None if not Found.
        :rtype: dict
        """
        for partition in candidate_queue['capacities']['queueCapacitiesByPartition']:
            if partition['partitionName'] == cluster_node_label:
                return partition
        return None

    def cluster_reservations(self, queue=None, reservation_id=None,
                             start_time=None, end_time=None,
                             include_resource_allocations=None):
        """
        The Cluster Reservation API can be used to list reservations. When listing reservations
        the user must specify the constraints in terms of a queue, reservation-id, start time or
        end time. The user must also specify whether or not to include the full resource allocations
        of the reservations being listed. The resulting page returns a response containing
        information related to the reservation such as the acceptance time, the user, the resource
        allocations, the reservation-id, as well as the reservation definition.

        :param str queue: the queue name containing the reservations to be listed. if not set, this
            value will default to “default”
        :param str reservation_id: the reservation-id of the reservation which will be listed. If
            this parameter is present, start-time and end-time will be ignored.
        :param str start_time:  reservations that end after this start-time will be listed. If
            unspecified or invalid, this will default to 0.
        :param str end_time: reservations that start after this end-time will be listed. If
            unspecified or invalid, this will default to Long.MaxValue.
        :param str include_resource_allocations: true or false. If true, the resource allocations
            of the reservation will be included in the response. If false, no resource allocations
            will be included in the response. This will default to false.
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/reservation/list'

        loc_args = (
            ('queue', queue),
            ('reservation-id', reservation_id),
            ('start-time', start_time),
            ('end-time', end_time),
            ('include-resource-allocations', include_resource_allocations)
        )

        params = self.construct_parameters(loc_args)

        return self.request(path, params=params)

    def cluster_new_delegation_token(self, renewer):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

        API to create delegation token.

        All delegation token requests must be carried out on a Kerberos
        authenticated connection(using SPNEGO). Carrying out operations on a non-kerberos
        connection will result in a FORBIDDEN response. In case of renewing a token, only
        the renewer specified when creating the token can renew the token. Other users(including
        the owner) are forbidden from renewing tokens.

        :param str renewer: The user who is allowed to renew the delegation token
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/delegation-token'

        return self.request(path, 'POST', json={"renewer": renewer})

    def cluster_renew_delegation_token(self, delegation_token):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

        API to renew delegation token.

        All delegation token requests must be carried out on a Kerberos
        authenticated connection(using SPNEGO). Carrying out operations on a non-kerberos
        connection will result in a FORBIDDEN response. In case of renewing a token, only
        the renewer specified when creating the token can renew the token. Other users(including
        the owner) are forbidden from renewing tokens.

        :param str delegation_token: Delegation token
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/delegation-token/expiration'

        return self.request(path, 'POST', headers={
            "Hadoop-YARN-RM-Delegation-Token": delegation_token
        })

    def cluster_cancel_delegation_token(self, delegation_token):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

        API to cancel delegation token.

        All delegation token requests must be carried out on a Kerberos
        authenticated connection(using SPNEGO). Carrying out operations on a non-kerberos
        connection will result in a FORBIDDEN response.

        :param str delegation_token: Delegation token
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/delegation-token'

        return self.request(path, 'DELETE', headers={
            "Hadoop-YARN-RM-Delegation-Token": delegation_token
        })

    def cluster_new_reservation(self):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

        Use the New Reservation API, to obtain a reservation-id which can then be used as part of
        the Cluster Reservation API Submit to submit reservations.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/reservation/new-reservation'

        return self.request(path, 'POST')

    def cluster_submit_reservation(self, data):
        """
        The Cluster Reservation API can be used to submit reservations. When submitting a
        reservation the user specifies the constraints in terms of resources, and time that is
        required. The resulting response is successful if the reservation can be made. If a
        reservation-id is used to submit a reservation multiple times, the request will succeed
        if the reservation definition is the same, but only one reservation will be created. If
        the reservation definition is different, the server will respond with an error response.
        When the reservation is made, the user can use the reservation-id used to submit the
        reservation to get access to the resources by specifying it as part of Cluster Submit
        Applications API.

        For data body definition refer to:
        (https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Reservation_API_Submit)

        :param dict data: Reservation details
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/reservation/submit'

        return self.request(path, 'POST', json=data)

    def cluster_update_reservation(self, data):
        """
        The Cluster Reservation API Update can be used to update existing reservations.Update of a
        Reservation works similarly to submit described above, but the user submits the
        reservation-id of an existing reservation to be updated. The semantics is a try-and-swap,
        successful operation will modify the existing reservation based on the requested update
        parameter, while a failed execution will leave the existing reservation unchanged.

        For data body definition refer to:
        (https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Reservation_API_Update)

        :param dict data: Reservation details
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/reservation/update'

        return self.request(path, 'POST', json=data)

    def cluster_delete_reservation(self, reservation_id):
        """
        The Cluster Reservation API Update can be used to update existing reservations.Update of a
        Reservation works similarly to submit described above, but the user submits the
        reservation-id of an existing reservation to be updated. The semantics is a try-and-swap,
        successful operation will modify the existing reservation based on the requested update
        parameter, while a failed execution will leave the existing reservation unchanged.

        :param str reservation_id: The id of the reservation to be deleted (the system automatically
            looks up the right queue from this)
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/reservation/delete'

        return self.request(path, 'POST', json={'reservation-id': reservation_id})

    def cluster_application_timeouts(self, application_id):
        """
        Cluster Application Timeouts API can be used to get all configured timeouts of an
        application. When you run a GET operation on this resource, a collection of timeout objects
        is returned. Each timeout object is composed of a timeout type, expiry-time and remaining
        time in seconds.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/timeouts'.format(
            appid=application_id)

        return self.request(path)

    def cluster_application_timeout(self, application_id, timeout_type):
        """
        The Cluster Application Timeout resource contains information about timeout.

        :param str application_id: The application id
        :param str timeout_type: Timeout type. Valid values are the members of the
            ApplicationTimeoutType enum. LIFETIME is currently the only valid value. .
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/timeouts/{timeout_type}'.format(
            appid=application_id, timeout_type=timeout_type)

        return self.request(path)

    def cluster_update_application_timeout(self, application_id, timeout_type, expiry_time):
        """
        Update timeout of an application for given timeout type.

        :param str application_id: The application id
        :param str timeout_type: Timeout type. Valid values are the members of the
            ApplicationTimeoutType enum. LIFETIME is currently the only valid value.
        :param str expiry_time: Time at which the application will expire in
            ISO8601 yyyy-MM-dd’T’HH:mm:ss.SSSZ format.
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/apps/{appid}/timeout'.format(appid=application_id)

        return self.request(path, 'PUT', json={
            "timeout": {"type": timeout_type, "expiryTime": expiry_time}
        })

    def cluster_scheduler_conf_mutation(self):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

        API to retrieve the scheduler’s configuration that is currently loaded into
        scheduler’s context.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/scheduler-conf'

        return self.request(path)

    def cluster_modify_scheduler_conf_mutation(self, data):
        """
        (This feature is currently in the alpha stage and may change in the
        future)

        API to modify the scheduler configuration

        For data body definition refer to:
        (https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Scheduler_Configuration_Mutation_API)

        :param dict data: sched-conf dictionary object
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = '/ws/v1/cluster/scheduler-conf'

        return self.request(path, 'PUT', json=data)
