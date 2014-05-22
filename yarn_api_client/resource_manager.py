# -*- coding: utf-8 -*-
from .base import BaseYarnAPI
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

        return self.request(path)

    def cluster_node(self, node_id):
        path = '/ws/v1/cluster/nodes/{nodeid}'.format(nodeid=node_id)

        return self.request(path)
