# -*- coding: utf-8 -*-
from .base import BaseYarnAPI


class NodeManager(BaseYarnAPI):
    def __init__(self, address=None, port=8042, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout

    def node_information(self):
        path = '/ws/v1/node/info'
        return self.request(path)

    def node_applications(self, state=None, user=None):
        path = '/ws/v1/node/apps'

        loc_args = (
            ('state', state),
            ('user', user))

        params = {key: value for key, value in loc_args if value is not None}

        return self.request(path, **params)

    def node_application(self, application_id):
        path = '/ws/v1/node/apps/{appid}'.format(appid=application_id)

        return self.request(path)

    def node_containers(self):
        path = '/ws/v1/node/containers'

        return self.request(path)

    def node_container(self, container_id):
        path = '/ws/v1/node/containers/{containerid}'.format(
            containerid=container_id)

        return self.request(path)
