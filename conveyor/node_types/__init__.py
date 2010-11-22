from __future__ import absolute_import

import json

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, id, init_tuple=tuple()):
        """Create attributes from tuple data"""

        self.id = id
        self.path = get_path(self.__class__, id)

        if len(init_tuple) > 0:
            try:
                self.data.update(json.loads(init_tuple[0]))
            except:
                log.error('Unable to unserialize JSON in %s: %s', self.path, init_tuple[0])
            for name,value in init_tuple[1].items():
               setattr(self, name, value)

    def write(self, handle):
        """Create a node in ZooKeeper"""

        path = get_path(self.__class__, self.id)
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


def get_path(type, id=''):
    """Return the absolute path for the specified type/id"""

    if type == Application:
        root = zookeeper.ZK_PATH_SEP + 'apps'
    elif type == Host:
        root = zookeeper.ZK_PATH_SEP + 'hosts'

    if id != '':
        result = root + zookeeper.ZK_PATH_SEP + id
    else:
        result = root
    return result


def list_nodes(handle, type):
    """Return a sorted list of nodes of the specified type"""

    path = get_path(type)

    result = sorted(zookeeper.get_children(handle, path))
    log.debug('Listing nodes of type %s: %s ', type, ', '.join(result))
    return result


def read_node(handle, type, id):
    """Read a node from ZooKeeper"""

    path = get_path(type, id)

    node_tuple = zookeeper.get(handle, path)
    log.debug('Read instance of %s: %s (%s)', type, path, node_tuple)

    return type(id=id, init_tuple=node_tuple)


def read_nodes(handle, type, groups=set(), watcher=None):
    """Return all nodes of the specified type. If groups are specified, only return nodes in the specified groups."""

    nodes = dict()
    path = get_path(type)
    for id in zookeeper.get_children(handle,path, watcher):
        node = read_node(handle=handle, type=type, id=id)
        if len(groups) > 0:
            if node.in_groups(groups):
                nodes[id] = node
            else:
                log.debug('Node is not in my group(s) (ignoring): %s', id)
        else:
            nodes[id] = node
    return nodes


def delete_node(handle, type, id):
    """Delete a node from ZooKeeper"""

    path = get_path(type, id)

    log.debug('Deleting instance of %s: %s', type, path)
    return zookeeper.delete(handle, path)
