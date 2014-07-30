# -*- coding: utf-8 -*-
from .base import BaseYarnAPI
from .constants import ApplicationState
from .errors import IllegalArgumentError


class NodeManager(BaseYarnAPI):
    """
    The NodeManager REST API's allow the user to get status on the node and
    information about applications and containers running on that node.
    """
    def __init__(self, address=None, port=8042, timeout=30):
        self.address, self.port, self.timeout = address, port, timeout

    def node_information(self):
        """
        The node information resource provides overall information about that
        particular node.
        """
        path = '/ws/v1/node/info'
        return self.request(path)

    def node_applications(self, state=None, user=None):
        """
        With the Applications API, you can obtain a collection of resources,
        each of which represents an application.
        """
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
        """
        An application resource contains information about a particular
        application that was run or is running on this NodeManager.
        """
        path = '/ws/v1/node/apps/{appid}'.format(appid=application_id)

        return self.request(path)

    def node_containers(self):
        """
        With the containers API, you can obtain a collection of resources,
        each of which represents a container.
        """
        path = '/ws/v1/node/containers'

        return self.request(path)

    def node_container(self, container_id):
        """
        A container resource contains information about a particular container
        that is running on this NodeManager.
        """
        path = '/ws/v1/node/containers/{containerid}'.format(
            containerid=container_id)

        return self.request(path)
