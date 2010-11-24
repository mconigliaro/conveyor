from __future__ import absolute_import

import json

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, id, data={}, attrs={}):
        """Create attributes from tuple data"""

        self.id = id
        self.path = self.get_path(id)
        self.data = data
        for name,value in attrs.items():
           setattr(self, name, value)

    @classmethod
    def get_path(self, id=None):
        """Return the absolute path for a node"""

        path = zookeeper.ZK_PATH_SEP + self.__name__.lower() + 's'

        if id != None:
            path += zookeeper.ZK_PATH_SEP + id

        return path

    @classmethod
    def list(self, handle, id=None):
        """Return a sorted list of nodes of the specified type"""

        path = self.get_path(id)

        result = sorted(zookeeper.get_children(handle, path))
        log.debug('Listing nodes of type %s (%s): %s ', self, path, ', '.join(result))
        return result

    @classmethod
    def read(self, handle, id):
        """Read a node from ZooKeeper"""

        path = self.get_path(id)

        node_tuple = zookeeper.get(handle, path)
        log.debug('Read instance of %s: %s %s', self, path, node_tuple)

        try:
            data = json.loads(node_tuple[0])
        except:
            log.error('Unable to unserialize JSON in %s: %s', self.path, node_tuple[0])
            data = None

        return self(id=id, data=data, attrs=node_tuple[1])

    @classmethod
    def read_all(self, handle, groups=set(), watcher=None):
        """Return all nodes of the specified type. If groups are specified, only return nodes in the specified groups."""

        nodes = []
        path = self.get_path()
        for id in zookeeper.get_children(handle,path, watcher):
            node = self.read(handle=handle, id=id)
            if len(groups) > 0:
                if node.in_groups(groups):
                    nodes.append(node)
                else:
                    log.debug('Node is not in my group(s) (ignoring): %s', id)
            else:
                nodes.append(node)

        return nodes

    @classmethod
    def delete(self, handle, id):
        """Delete a node from ZooKeeper"""

        path = self.get_path(id)

        log.debug('Deleting instance of %s: %s', self, path)
        return zookeeper.delete(handle, path)

    def in_groups(self, groups=set()):
        """Return true if the node belongs to any of the specified groups"""

        result = False
        if set(groups) & set(self.data['groups']):
            result = True
        return result


class EphemeralNode(Node):

    def __init__(self, id, data={}, attrs={}):
        super(EphemeralNode, self).__init__(id=id, data=data, attrs=attrs)

    def write(self, handle):
        """Create an ephemeral node in ZooKeeper"""

        while True:
            try:
                zookeeper.create(handle, self.path, json.dumps(self.data), [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
                log.debug('Wrote instance of %s: %s (%s)', self.__class__, self.path, self.data)
                break
            except zookeeper.NodeExistsException:
                zookeeper.delete(handle, self.path)
                log.debug('Deleted old instance of %s: %s', self.__class__, self.path)
            except zookeeper.NoNodeException:
                zookeeper.create_r(handle, zookeeper.get_parent_node(self.path), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

        return self


class PersistentNode(Node):

    def __init__(self, id, data={}, attrs={}):
        super(PersistentNode, self).__init__(id=id, data=data, attrs=attrs)

    def write(self, handle):
        """Create a persistent node in ZooKeeper"""

        while True:
            try:
                zookeeper.create(handle, self.path, json.dumps(self.data), [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)
                log.debug('Wrote instance of %s: %s (%s)', self.__class__, self.path, self.data)
                break
            except zookeeper.NodeExistsException:
                zookeeper.delete(handle, self.path)
                log.debug('Deleted old instance of %s: %s', self.__class__, self.path)
            except zookeeper.NoNodeException:
                zookeeper.create_r(handle, zookeeper.get_parent_node(self.path), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

        return self


class Host(EphemeralNode):
    """Host node class"""

    def __init__(self, id, data={}, attrs={}):
        data['groups'] = data.get('groups', [])
        super(self.__class__, self).__init__(id=id, data=data, attrs=attrs)


class Application(PersistentNode):
    """Application node class"""

    def __init__(self, id, data={}, attrs={}):
        data['groups'] = data.get('groups', [])
        data['version'] = data.get('version', '0.0')
        super(self.__class__, self).__init__(id=id, data=data, attrs=attrs)

    def version_greater_than(self, version='0'):
        """Return True if the application's version is greater than the given version"""

        result = False
        if self.data['version'] > version:
            result = True
        return result


class Deployment(PersistentNode):
    """Deployment node class"""

    def __init__(self, id, data={}, attrs={}):
        data['slots'] = data.get('slots', 1)
        data['completed'] = data.get('completed', 0)
        data['failed'] = data.get('failed', 0)
        super(self.__class__, self).__init__(id=id, data=data, attrs=attrs)

    def write(self, handle):
        """Create a deployment node in ZooKeeper"""

        while True:
            try:
                zookeeper.create(handle, self.path, json.dumps(self.data), [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)
                log.info('Wrote instance of %s: %s (%s)', self.__class__, self.path, self.data)
                break
            except zookeeper.NoNodeException:
                zookeeper.create_r(handle, zookeeper.get_parent_node(self.path), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

        return self

    def has_free_slot(self, handle):
        """Return True if the deployment has free slots"""

        result = False
        if self.data['slots'] < len(DeploymentSlot.list(handle=handle)):
            result = True
        return result


class DeploymentSlot(EphemeralNode):
    """Deployment slot node class"""

    class NoFreeSlots(Exception):
        """Exception raised when no free slots are available"""
        pass

    def __init__(self, id, data={}, attrs={}):
        super(self.__class__, self).__init__(id=id, data=data, attrs=attrs)

    @classmethod
    def get_path(self, id=None):
        """Return the absolute path for a deployment slot node"""

        path = zookeeper.ZK_PATH_SEP + 'deployments'

        if id != None:
            path += zookeeper.ZK_PATH_SEP + id

        return path

    def occupy(self, handle):

        deployment_id = zookeeper.get_parent_node(self.id)
        try:
            deployment = Deployment(id=deployment_id).write(handle=handle)
        except zookeeper.NodeExistsException:
            deployment = Deployment.read(handle=handle, id=deployment_id)

        self.write(handle)
        if deployment.data['slots'] < len(DeploymentSlot.list(handle=handle, id=deployment_id)):
            raise DeploymentSlot.NoFreeSlots

    @classmethod
    def free(self, handle, id, result):

        deployment = self.read(handle=handle, id=zookeeper.get_parent_node(id))
        new_data = deployment.data

        new_data['slots'] += 1
        if result:
            new_data['completed'] += 1
        else:
            new_data['failed'] += 1


        self.delete(handle=handle, id=id)

        try:
            Deployment.delete(handle=handle, id=zookeeper.get_parent_node(id))
        except zookeeper.NodeExistsException:
            pass
