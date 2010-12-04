from __future__ import absolute_import

from distutils import version
import json
import logging
import re

from . import zookeeper


def list(handle, path, watcher=None):
    """Return a sorted list of child nodes from ZooKeeper"""

    result = sorted(zookeeper.get_children(handle, path, watcher))
    logging.getLogger().debug('Listing children of %s: %s ', path, ', '.join(result))
    return result


def delete(handle, path):
    """Delete a node from ZooKeeper"""

    logging.getLogger().debug('Deleting %s', path)
    return zookeeper.delete(handle, path)


class Node(object):
    """Base class for all nodes"""

    def __init__(self, path, data={}, attrs={}):
        """Create a new node using the supplied data/attributes"""

        self.path = path
        self.data = data

        for name,value in attrs.items():
           setattr(self, name, value)

    @classmethod
    def read(self, handle, path, watcher=None):
        """Read a node from ZooKeeper"""

        node_tuple = zookeeper.get(handle, path, watcher)
        logging.getLogger().debug('Read instance of %s: %s %s', self.__name__, path, node_tuple)

        try:
            data = json.loads(node_tuple[0])
        except:
            logging.getLogger().error('Unable to unserialize JSON from %s: %s', path, node_tuple[0])
            data = None

        return self(path=path, data=data, attrs=node_tuple[1])

    @classmethod
    def read_all(self, handle, path, groups=set(), root_watcher=None, child_watcher=None):
        """Read all child nodes from ZooKeeper. If groups are specified, only return nodes in the specified groups."""

        nodes = []
        for name in zookeeper.get_children(handle, path, root_watcher):
            node = self.read(handle=handle, path=zookeeper.path_join('applications', name), watcher=child_watcher)
            if len(groups) > 0:
                if node.in_groups(groups):
                    nodes.append(node)
                else:
                    logging.getLogger().debug('Node is not in my group(s) (ignoring): %s', name)
            else:
                nodes.append(node)

        return nodes

    def in_groups(self, groups):
        """Return true if the node belongs to any of the specified groups"""

        result = False
        if set(groups) & set(self.data['groups']):
            result = True
        return result

    def write(self, handle, acl, flags, overwrite=True, overwrite_if_version=None):
        """Create a persistent node in ZooKeeper"""

        while True:
            try:
                zookeeper.create(handle, self.path, json.dumps(self.data), acl, flags)
                logging.getLogger().debug('Wrote instance of %s: %s (%s)', self.__class__.__name__, self.path, self.data)
                break
            except zookeeper.NodeExistsException:
                if overwrite:
                    if overwrite_if_version:
                        zookeeper.set(handle, self.path, json.dumps(self.data), overwrite_if_version)
                    else:
                        zookeeper.set(handle, self.path, json.dumps(self.data))
                    logging.getLogger().debug('Updated instance of %s: %s (%s)', self.__class__.__name__, self.path, self.data)
                    break
                else:
                    raise
            except zookeeper.NoNodeException:
                zookeeper.create_r(handle, zookeeper.get_parent_node(self.path), '', acl, zookeeper.PERSISTENT)

        return self


class EphemeralNode(Node):
    """Ephemeral node class"""

    def __init__(self, path, data={}, attrs={}):
        super(EphemeralNode, self).__init__(path=path, data=data, attrs=attrs)

    def write(self, handle, acl=[zookeeper.ZOO_OPEN_ACL_UNSAFE], overwrite=True, overwrite_if_version=None):
        """Create an ephemeral node in ZooKeeper"""

        return super(EphemeralNode, self).write(handle=handle, acl=acl, flags=zookeeper.EPHEMERAL, overwrite=overwrite, overwrite_if_version=overwrite_if_version)


class PersistentNode(Node):
    """Persistent node class"""

    def __init__(self, path, data={}, attrs={}):
        super(PersistentNode, self).__init__(path=path, data=data, attrs=attrs)

    def write(self, handle, acl=[zookeeper.ZOO_OPEN_ACL_UNSAFE], overwrite=True, overwrite_if_version=None):
        """Create a persistent node in ZooKeeper"""

        return super(PersistentNode, self).write(handle=handle, acl=acl, flags=zookeeper.PERSISTENT, overwrite=overwrite, overwrite_if_version=overwrite_if_version)


class Host(EphemeralNode):
    """Host node class"""

    def __init__(self, path, data={}, attrs={}):

        self.id = zookeeper.path_split(path)[-1]

        data = {
            'groups': data.get('groups', [])
        }

        super(Host, self).__init__(path=path, data=data, attrs=attrs)


class Application(PersistentNode):
    """Application node class"""

    class DeploymentSlotOverflow(Exception):
        """Exception raised on deployment slot overflow"""

    def __init__(self, path, data={}, attrs={}):

        self.id = zookeeper.path_split(path)[-1]

        data = {
            'groups': data.get('groups', []),
            'version': data.get('version', '0.0'),
            'deployment_slot_increment': data.get('deployment_slot_increment', 1),
            'deployment_slots': data.get('deployment_slots', 1),
            'deployment_completed': data.get('deployment_completed', 0),
            'deployment_failed': data.get('deployment_failed', 0)
        }

        try:
            data['version'] = str(version.StrictVersion(str(data['version'])))
        except ValueError:
            data['version'] = '0.0'

        super(Application, self).__init__(path=path, data=data, attrs=attrs)

    def deployment_slot_overflow(self, handle):
        """Return True on deployment slot overflow"""

        result = False
        if len(list(handle=handle, path=self.path)) > self.data['deployment_slots']:
            result = True
        return result


class DeploymentSlot(EphemeralNode):
    """Deployment slot node class"""

    def __init__(self, path, data={}, attrs={}):

        self.id = zookeeper.path_join(*zookeeper.path_split(path)[-2:])

        super(DeploymentSlot, self).__init__(path=path, data=data, attrs=attrs)

    def occupy(self, handle):
        """Occupy a free deployment slot"""

        self.write(handle=handle)

        while True:
            try:
                app = Application.read(handle=handle, path=zookeeper.get_parent_node(self.path))

                if app.deployment_slot_overflow(handle=handle):
                    delete(handle=handle, path=self.path)
                    raise Application.DeploymentSlotOverflow
                else:
                    app.data['deployment_slots'] -= 1
                    app.write(handle=handle, overwrite_if_version=app.version)
                    break

            except zookeeper.BadVersionException:
                logging.getLogger().debug('Version mismatch (retrying)')

    @classmethod
    def free(self, handle, path, app_handler_result):
        """Free up a deployment slot"""

        delete(handle=handle, path=path)

        while True:
            try:
                app = Application.read(handle=handle, path=zookeeper.get_parent_node(path))

                if app_handler_result:
                    app.data['deployment_completed'] += 1
                else:
                    app.data['deployment_failed'] += 1
                app.data['deployment_slots'] += app.data['deployment_slot_increment']

                app.write(handle=handle, overwrite_if_version=app.version)
                break

            except zookeeper.BadVersionException:
                logging.getLogger().debug('Version mismatch (retrying)')
