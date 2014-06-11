# -*- coding: utf-8 -*-
from .base import BaseYarnAPI
from .constants import ApplicationState
from .errors import IllegalArgumentError


class NodeManager(BaseYarnAPI):
    def __init__(self, address=None, port=8042, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout

    def node_information(self):
        path = '/ws/v1/node/info'
        return self.request(path)

    def node_applications(self, state=None, user=None):
        path = '/ws/v1/node/apps'

        legal_states = set([s for s, _ in ApplicationState])
        if state is not None and state not in legal_states:
            msg = 'Application State %s is illegal' % (state,)
            raise IllegalArgumentError(msg)

        loc_args = (
            ('state', state),
            ('user', user))

        params = self.construct_parameters(loc_args)

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
