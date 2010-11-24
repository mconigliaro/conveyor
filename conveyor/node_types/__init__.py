from __future__ import absolute_import

import json

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, id, data={}, attrs={}):
        """Create a new node using the supplied data/attributes"""

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
    def list(self, handle, id=None, watcher=None):
        """Return a sorted list of nodes of the specified type"""

        path = self.get_path(id)

        result = sorted(zookeeper.get_children(handle, path, watcher))
        log.debug('Listing nodes of type %s (%s): %s ', self.__name__, path, ', '.join(result))
        return result

    @classmethod
    def read(self, handle, id, watcher=None):
        """Read a node from ZooKeeper"""

        path = self.get_path(id)

        node_tuple = zookeeper.get(handle, path, watcher)
        log.debug('Read instance of %s: %s %s', self.__name__, path, node_tuple)

        try:
            data = json.loads(node_tuple[0])
        except:
            log.error('Unable to unserialize JSON in %s: %s', path, node_tuple[0])
            data = None

        return self(id=id, data=data, attrs=node_tuple[1])

    @classmethod
    def read_all(self, handle, groups=set(), watcher=None):
        """Return all nodes of the specified type. If groups are specified, only return nodes in the specified groups."""

        nodes = []
        path = self.get_path()
        for id in zookeeper.get_children(handle,path, watcher):
            node = self.read(handle=handle, id=id, watcher=watcher)
            if len(groups) > 0:
                if node.in_groups(groups):
                    nodes.append(node)
                else:
                    log.debug('Node is not in my group(s) (ignoring): %s', id)
            else:
                nodes.append(node)

        return nodes

    def write(self, handle, acl, flags, overwrite=True):
        """Create a persistent node in ZooKeeper"""

        while True:
            try:
                zookeeper.create(handle, self.path, json.dumps(self.data), acl, flags)
                log.debug('Wrote instance of %s: %s (%s)', self.__class__.__name__, self.path, self.data)
                break
            except zookeeper.NodeExistsException:
                if overwrite:
                    zookeeper.delete(handle, self.path)
                    log.debug('Deleted old instance of %s: %s', self.__class__.__name__, self.path)
                else:
                    raise
            except zookeeper.NoNodeException:
                zookeeper.create_r(handle, zookeeper.get_parent_node(self.path), '', acl, zookeeper.PERSISTENT)

        return self

    @classmethod
    def delete(self, handle, id):
        """Delete a node from ZooKeeper"""

        path = self.get_path(id)

        log.debug('Deleting instance of %s: %s', self.__name__, path)
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

    def write(self, handle, acl=[zookeeper.ZOO_OPEN_ACL_UNSAFE], overwrite=True):
        """Create an ephemeral node in ZooKeeper"""

        return super(EphemeralNode, self).write(handle=handle, acl=acl, flags=zookeeper.EPHEMERAL, overwrite=overwrite)


class PersistentNode(Node):

    def __init__(self, id, data={}, attrs={}):
        super(PersistentNode, self).__init__(id=id, data=data, attrs=attrs)

    def write(self, handle, acl=[zookeeper.ZOO_OPEN_ACL_UNSAFE], overwrite=True):
        """Create a persistent node in ZooKeeper"""

        return super(PersistentNode, self).write(handle=handle, acl=acl, flags=zookeeper.PERSISTENT, overwrite=overwrite)


class Host(EphemeralNode):
    """Host node class"""

    def __init__(self, id, data={}, attrs={}):
        data['groups'] = data.get('groups', [])
        super(Host, self).__init__(id=id, data=data, attrs=attrs)


class Application(PersistentNode):
    """Application node class"""

    def __init__(self, id, data={}, attrs={}):
        data['groups'] = data.get('groups', [])
        data['version'] = data.get('version', '0.0')
        super(Application, self).__init__(id=id, data=data, attrs=attrs)


class Deployment(PersistentNode):
    """Deployment node class"""

    def __init__(self, id, data={}, attrs={}):
        data['slots'] = data.get('slots', 1)
        data['completed'] = data.get('completed', 0)
        data['failed'] = data.get('failed', 0)
        super(Deployment, self).__init__(id=id, data=data, attrs=attrs)

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
        super(DeploymentSlot, self).__init__(id=id, data=data, attrs=attrs)

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
            deployment = Deployment(id=deployment_id).write(handle=handle, overwrite=False)
        except zookeeper.NodeExistsException:
            deployment = Deployment.read(handle=handle, id=deployment_id)

        self.write(handle=handle)
        if deployment.has_free_slot(handle=handle):
            self.delete(handle=handle, id=self.id)
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
