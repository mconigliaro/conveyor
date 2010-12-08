from __future__ import absolute_import

import json
import logging
import re
import subprocess

from . import zookeeper


def list_children(handle, path, watcher=None):
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
                nodes.append(node)

        return nodes

    def in_groups(self, groups):
        """Return true if the node belongs to any of the specified groups"""

        intersection = list(set(groups) & set(self.data['groups']))
        logging.getLogger().debug('Intersection between groups %s and %s: %s', groups, self.data['groups'], intersection)

        if intersection:
            result = True
        else:
            result = False

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

    class CommandError(Exception):
        """Exception raised on command error"""

    def __init__(self, path, data={}, attrs={}):

        self.id = zookeeper.path_split(path)[-1]

        data = {
            'groups': data.get('groups', []),
            'version': data.get('version', '0'),
            'slot_increment': data.get('slot_increment', 1),
            'slots': data.get('slots', 1),
            'completed': data.get('completed', []),
            'failed': data.get('failed', [])
        }

        super(Application, self).__init__(path=path, data=data, attrs=attrs)

    def deployment_slot_overflow(self, handle):
        """Return True on deployment slot overflow"""

        result = False
        if len(list_children(handle=handle, path=self.path)) > self.data['slots']:
            result = True
        return result

    def deployed(self, host_id):
        """Return True if this application has already been deployed"""

        if host_id in set(self.data['failed'] + self.data['completed']):
            logging.getLogger().debug('Deployment of %s %s has already been recorded for host %s', self.id, self.data['version'], host_id)
            result = True
        else:
            logging.getLogger().debug('Deployment of %s %s has NEVER been recorded for host %s', self.id, self.data['version'], host_id)
            result = False

        return result

    def run_command(self, command):
        """Run a command using this node's data/attributes"""

        command = self.__interpolate(command)
        logging.getLogger().debug('Running command: %s', command)
        p = subprocess.Popen(command, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        result = p.communicate()[0].strip()

        if p.returncode:
            logging.getLogger().warn('Command result: %s (%d)', result, p.returncode)
            raise Application.CommandError
        else:
            logging.getLogger().debug('Command result: %s (%d)', result, p.returncode)

        return result

    def __interpolate(self, command):
        """Do variable interpolation on a string using this node's data"""

        def get_attr(match_obj):
            """Return an attribute value"""

            item = match_obj.group(1)
            if hasattr(self, item):
                result = getattr(self, item)
            else:
                result = ''
            return str(result)

        def get_data(match_obj):
            """Return a data value"""

            item = match_obj.group(1)
            if item in self.data:
                result = self.data[item]
            else:
                result = ''
            return str(result)

        return re.sub('%\((.+?)\)s', get_attr, re.sub('%\(data\[(.+?)]\)s', get_data, command))


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
                    app.data['slots'] -= 1
                    app.write(handle=handle, overwrite_if_version=app.version)
                    break

            except zookeeper.BadVersionException:
                logging.getLogger().debug('Version mismatch (retrying)')

    @classmethod
    def free(self, handle, path, deploy_result):
        """Free up a deployment slot"""

        delete(handle=handle, path=path)

        host_id = zookeeper.path_split(path)[-1]

        while True:
            try:
                app = Application.read(handle=handle, path=zookeeper.get_parent_node(path))

                if deploy_result:
                    result = 'completed'
                    logging.getLogger().info('Deployment of %s %s recorded as: %s', app.id, app.data['version'], result)

                else:
                    result = 'failed'
                    logging.getLogger().error('Deployment of %s %s recorded as: %s', app.id, app.data['version'], result)

                if host_id not in app.data[result]:
                    app.data[result].extend([host_id])

                app.data['slots'] += app.data['slot_increment']

                app.write(handle=handle, overwrite_if_version=app.version)
                break

            except zookeeper.BadVersionException:
                logging.getLogger().debug('Version mismatch (retrying)')
