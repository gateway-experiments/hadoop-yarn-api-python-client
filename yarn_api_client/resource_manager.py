# -*- coding: utf-8 -*-
from .base import BaseYarnAPI
from .constants import YarnApplicationState, FinalApplicationStatus
from .errors import IllegalArgumentError
from .hadoop_conf import get_resource_manager_host_port


class ResourceManager(BaseYarnAPI):
    def __init__(self, address=None, port=8088, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout
        if address is None:
            self.logger.debug(u'Get configuration from hadoop conf dir')
            address, port = get_resource_manager_host_port()
            self.address, self.port = address, port

    def cluster_information(self):
        path = '/ws/v1/cluster/info'
        return self.request(path)

    def cluster_metrics(self):
        path = '/ws/v1/cluster/metrics'
        return self.request(path)

    def cluster_scheduler(self):
        path = '/ws/v1/cluster/scheduler'
        return self.request(path)

    def cluster_applications(self, state=None, final_status=None,
                             user=None, queue=None, limit=None,
                             started_time_begin=None, started_time_end=None,
                             finished_time_begin=None, finished_time_end=None):
        path = '/ws/v1/cluster/apps'

        legal_states = set([s for s, _ in YarnApplicationState])
        if state is not None and state not in legal_states:
            msg = u'Yarn Application State %s is illegal' % (state,)
            raise IllegalArgumentError(msg)

        legal_final_statuses = set([s for s, _ in FinalApplicationStatus])
        if final_status is not None and final_status not in legal_final_statuses:
            msg = u'Final Application Status %s is illegal' % (final_status,)
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

        params = {key: value for key, value in loc_args if value is not None}

        return self.request(path, **params)

    def cluster_application_statistics(self, state_list=None,
                                       application_type_list=None):
        """This method work in Hadoop > 2.0.0
        """
        path = '/ws/v1/cluster/appstatistics'

        # TODO: validate state argument
        states = ','.join(state_list) if state_list is not None else None
        if application_type_list is not None:
            applicationTypes = ','.join(application_type_list)
        else:
            applicationTypes = None

        loc_args = (
            ('states', states),
            ('applicationTypes', applicationTypes))
        params = {key: value for key, value in loc_args if value is not None}

        return self.request(path, **params)

    def cluster_application(self, application_id):
        path = '/ws/v1/cluster/apps/{appid}'.format(appid=application_id)

        return self.request(path)

    def cluster_application_attempts(self, application_id):
        path = '/ws/v1/cluster/apps/{appid}/appattempts'.format(
            appid=application_id)

        return self.request(path)

    def cluster_nodes(self, state=None, healthy=None):
        path = '/ws/v1/cluster/nodes'
        # TODO: validate state argument

        legal_healthy = ['true', 'false']
        if healthy is not None and healthy not in legal_healthy:
            msg = u'Valid Healthy arguments are true, false'
            raise IllegalArgumentError(msg)

        loc_args = (
            ('state', state),
            ('healthy', healthy),
        )
        params = {key: value for key, value in loc_args if value is not None}

        return self.request(path, **params)

    def cluster_node(self, node_id):
        path = '/ws/v1/cluster/nodes/{nodeid}'.format(nodeid=node_id)

        return self.request(path)
