from __future__ import absolute_import

import json

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, id, init_tuple=tuple()):
        """Create attributes from tuple data"""

        self.id = id
        self.path = self.__class__.get_path(id=id)

        if len(init_tuple) > 0:
            try:
                self.data.update(json.loads(init_tuple[0]))
            except:
                log.error('Unable to unserialize JSON in %s: %s', self.path, init_tuple[0])
            for name,value in init_tuple[1].items():
               setattr(self, name, value)

    @classmethod
    def get_path(self, id=''):
        """Return the absolute path for the specified type/id"""

        if self == Application:
            root = zookeeper.ZK_PATH_SEP + 'apps'
        elif self == Host:
            root = zookeeper.ZK_PATH_SEP + 'hosts'

        if id != '':
            result = root + zookeeper.ZK_PATH_SEP + id
        else:
            result = root
        return result

    @classmethod
    def list(self, handle):
        """Return a sorted list of nodes of the specified type"""

        path = self.get_path()

        result = sorted(zookeeper.get_children(handle, path))
        log.debug('Listing nodes of type %s: %s ', self, ', '.join(result))
        return result

    @classmethod
    def read(self, handle, id):
        """Read a node from ZooKeeper"""

        path = self.get_path(id=id)

        node_tuple = zookeeper.get(handle, path)
        log.debug('Read instance of %s: %s (%s)', self, path, node_tuple)

        return self(id=id, init_tuple=node_tuple)

    @classmethod
    def read_all(self, handle, groups=set(), watcher=None):
        """Return all nodes of the specified type. If groups are specified, only return nodes in the specified groups."""

        nodes = dict()
        path = self.get_path()
        for id in zookeeper.get_children(handle,path, watcher):
            node = self.read(handle=handle, id=id)
            if node.in_groups(groups):
                nodes[id] = node
            else:
                log.debug('Node is not in my group(s) (ignoring): %s', id)

        return nodes

    @classmethod
    def delete(self, handle, id):
        """Delete a node from ZooKeeper"""

        path = self.get_path(id=id)

        log.debug('Deleting instance of %s: %s', self, path)
        return zookeeper.delete(handle, path)

    def write(self, handle):
        """Create a node in ZooKeeper"""

        path = self.__class__.get_path(id=self.id)
        if self.__class__ in [Host]:
            create_flag = zookeeper.EPHEMERAL
        else:
            create_flag = zookeeper.ZOO_PERSISTENT

        while True:
            try:
                zookeeper.create(handle, path, json.dumps(self.data), [zookeeper.ZOO_OPEN_ACL_UNSAFE], create_flag)
                log.info('Wrote instance of %s: %s (%s)', self.__class__, path, self.data)
                break
            except zookeeper.NodeExistsException:
                zookeeper.delete(handle, path)
                log.info('Deleted old instance of %s: %s', self.__class__, path)
            except zookeeper.NoNodeException:
                zookeeper.create_r(handle, zookeeper.get_parent_node(path), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

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
        super(self.__class__, self).__init__(id=id, init_tuple=init_tuple)


class Application(Node):
    """Application node class"""

    def __init__(self, id, groups=list(), version='0', init_tuple=tuple()):
        self.data = {
            'groups': groups,
            'version': version
        }
        super(self.__class__, self).__init__(id=id, init_tuple=init_tuple)

    def version_greater_than(self, version='0'):
        """Return true if the application's version is greater than the given version"""

        result = False
        if self.data['version'] > version:
            result = True
        return result
