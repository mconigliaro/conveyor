from __future__ import absolute_import

from distutils import version
import json
import re

from ..logging import log
from .. import zookeeper


class Node(object):
    """Base class for all nodes"""

    def __init__(self, id, data={}, attrs={}):
        """Create a new node using the supplied data/attributes"""

        self.id = id
        self.path = self.get_path(id=id)

        if 'version' in data:
            try:
                data['version'] = str(version.StrictVersion(str(data['version'])))
            except ValueError:
                data['version'] = '0.0'

        self.data = data

        for name,value in attrs.items():
           setattr(self, name, value)

    @classmethod
    def get_path(self, id=None):
        """Return the absolute path for a node"""

        if self.__name__ == 'DeploymentSlot':
            path = zookeeper.zkjoin(['applications'], absolute=True)
        else:
            path = zookeeper.zkjoin([self.__name__.lower() + 's'], absolute=True)

        if id:
            path += zookeeper.zkjoin([id], absolute=True)

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
    def read_all(self, handle, groups=set(), root_watcher=None, child_watcher=None):
        """Return all nodes of the specified type. If groups are specified, only return nodes in the specified groups."""

        nodes = []
        path = self.get_path()
        for id in zookeeper.get_children(handle, path, root_watcher):
            node = self.read(handle=handle, id=id, watcher=child_watcher)
            if len(groups) > 0:
                if node.in_groups(groups):
                    nodes.append(node)
                else:
                    log.debug('Node is not in my group(s) (ignoring): %s', id)
            else:
                nodes.append(node)

        return nodes

    def write(self, handle, acl, flags, overwrite=True, overwrite_if_version=None):
        """Create a persistent node in ZooKeeper"""

        while True:
            try:
                zookeeper.create(handle, self.path, json.dumps(self.data), acl, flags)
                log.debug('Wrote instance of %s: %s (%s)', self.__class__.__name__, self.path, self.data)
                break
            except zookeeper.NodeExistsException:
                if overwrite:
                    if overwrite_if_version:
                        zookeeper.set(handle, self.path, json.dumps(self.data), overwrite_if_version)
                    else:
                        zookeeper.set(handle, self.path, json.dumps(self.data))
                    log.debug('Updated instance of %s: %s (%s)', self.__class__.__name__, self.path, self.data)
                    break
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
    """Ephemeral node class"""

    def __init__(self, id, data={}, attrs={}):
        super(EphemeralNode, self).__init__(id=id, data=data, attrs=attrs)

    def write(self, handle, acl=[zookeeper.ZOO_OPEN_ACL_UNSAFE], overwrite=True, overwrite_if_version=None):
        """Create an ephemeral node in ZooKeeper"""

        return super(EphemeralNode, self).write(handle=handle, acl=acl, flags=zookeeper.EPHEMERAL, overwrite=overwrite, overwrite_if_version=overwrite_if_version)


class PersistentNode(Node):
    """Persistent node class"""

    def __init__(self, id, data={}, attrs={}):
        super(PersistentNode, self).__init__(id=id, data=data, attrs=attrs)

    def write(self, handle, acl=[zookeeper.ZOO_OPEN_ACL_UNSAFE], overwrite=True, overwrite_if_version=None):
        """Create a persistent node in ZooKeeper"""

        return super(PersistentNode, self).write(handle=handle, acl=acl, flags=zookeeper.PERSISTENT, overwrite=overwrite, overwrite_if_version=overwrite_if_version)


class Host(EphemeralNode):
    """Host node class"""

    def __init__(self, id, data={}, attrs={}):
        data['groups'] = data.get('groups', [])

        super(Host, self).__init__(id=id, data=data, attrs=attrs)


class Application(PersistentNode):
    """Application node class"""

    class DeploymentSlotOverflow(Exception):
        """Exception raised on deployment slot overflow"""

    def __init__(self, id, data={}, attrs={}):
        data['groups'] = data.get('groups', [])
        data['version'] = data.get('version', '0.0')
        data['deployment_slots'] = data.get('deployment_slots', 1)
        data['deployment_completed'] = data.get('deployment_completed', 0)
        data['deployment_failed'] = data.get('deployment_failed', 0)

        super(Application, self).__init__(id=id, data=data, attrs=attrs)

    def deployment_slot_overflow(self, handle):
        """Return True on deployment slot overflow"""

        result = False
        if len(DeploymentSlot.list(handle=handle, id=self.id)) > self.data['deployment_slots']:
            result = True
        return result


class DeploymentSlot(EphemeralNode):
    """Deployment slot node class"""

    class InvalidId(Exception):
        """Exception raised when using an invalid slot id (should look like: <application>/<host_id>)"""

    def __init__(self, id, data={}, attrs={}):
        if not re.match('.+%s.+' % (zookeeper.PATH_SEPARATOR), id):
            raise InvalidId

        super(DeploymentSlot, self).__init__(id=id, data=data, attrs=attrs)


    def occupy(self, handle):
        """Occupy a free deployment slot"""

        self.write(handle=handle)

        while True:
            try:
                app = Application.read(handle=handle, id=zookeeper.zksplit(self.id)[0])
                if app.deployment_slot_overflow(handle=handle):
                    self.delete(handle=handle, id=self.id)
                    raise Application.DeploymentSlotOverflow
                else:
                    app.data['deployment_slots'] -= 1
                    app.write(handle=handle, overwrite_if_version=app.version)
                    break

            except zookeeper.BadVersionException:
                log.debug('Version mismatch (retrying)')

    @classmethod
    def free(self, handle, id, app_handler_result, deployment_factor):
        """Free up a deployment slot"""

        self.delete(handle=handle, id=id)

        while True:
            try:
                app = Application.read(handle=handle, id=zookeeper.zksplit(id)[0])

                if app_handler_result:
                    app.data['deployment_completed'] += 1
                else:
                    app.data['deployment_failed'] += 1
                app.data['deployment_slots'] += deployment_factor

                app.write(handle=handle, overwrite_if_version=app.version)
                break

            except zookeeper.BadVersionException:
                    log.debug('Version mismatch (retrying)')
