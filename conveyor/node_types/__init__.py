from __future__ import absolute_import

import json

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, node_tuple):
        """Create attributes from tuple data"""

        try:
            self.data = json.loads(node_tuple[0])
        except TypeError:
            log.error('Unable to unserialize JSON: %s', node_tuple[0])
            self.data = dict()

        for name,value in node_tuple[1].items():
           setattr(self, name, value)

    def in_groups(self, groups=set()):
        """Return true if the node belongs to any of the specified groups"""

        result = False
        if set(groups) & set(self.data['groups']):
            result = True
        return result


class Host(Node):
    """Host node class"""

    def __init__(self, node_tuple):
        super(HostNode, self).__init__(node_tuple)


class Application(Node):
    """Application node class"""

    def __init__(self, node_tuple):
        super(Application, self).__init__(node_tuple)


def get_path(type, id=''):
    """Return the absolute path for the specified type/id"""

    if id != '':
        result = zookeeper.ZK_PATH_SEP + zookeeper.ZK_PATH_SEP.join([type, id])
    else:
        result = zookeeper.ZK_PATH_SEP + type
    return result
