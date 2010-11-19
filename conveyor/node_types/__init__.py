from __future__ import absolute_import

import json

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, id, type, init_tuple=tuple()):
        """Create attributes from tuple data"""

        self.id = id
        self.path = get_path(type, id)

        if len(init_tuple) > 0:
            try:
                self.data.update(json.loads(init_tuple[0]))
            except:
                log.error('Unable to unserialize JSON in %s: %s', self.path, init_tuple[0])
            for name,value in init_tuple[1].items():
               setattr(self, name, value)

    def in_groups(self, groups=set()):
        """Return true if the node belongs to any of the specified groups"""

        result = False
        if set(groups) & set(self.data['groups']):
            result = True
        return result


class Host(Node):
    """Host node class"""

    def __init__(self, id, groups=list(), init_tuple=tuple()):
        self.data = {
            'groups': groups
        }
        super(Host, self).__init__(id=id, type='hosts', init_tuple=init_tuple)


class Application(Node):
    """Application node class"""

    def __init__(self, id, groups=list(), version=0, init_tuple=tuple()):
        self.data = {
            'groups': groups,
            'version': version
        }
        super(Application, self).__init__(id=id, type='apps', init_tuple=init_tuple)

    def version_greater_than(self, version='0'):
        """Return true if the application's version is greater than the given version"""

        result = False
        if self.data['version'] > version:
            result = True
        return result


def get_path(type, id=''):
    """Return the absolute path for the specified type/id"""

    if id != '':
        result = zookeeper.ZK_PATH_SEP + zookeeper.ZK_PATH_SEP.join([type, id])
    else:
        result = zookeeper.ZK_PATH_SEP + type
    return result
