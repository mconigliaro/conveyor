import json
import os
import sys
import time
import threading

import zookeeper

from options import options, args
import zkplus
from log import log


class Conveyor(object):
    """The main Conveyor class"""

    def __init__(self, servers=options.servers, timeout=options.timeout, host_id=options.host_id, groups=options.groups):
        """Connect to a ZooKeeper ensamble"""
        self.host_id = host_id
        self.host_data = {
            'groups': groups
        }

        self.cv = threading.Condition()
        self.state = None

        self.cv.acquire()
        log.info('Connecting to ZooKeeper: %s' % servers)
        try:
            self.handle = zkplus.init(servers, self.init_watcher, timeout * 1000)
            while self.state != zkplus.CONNECTED_STATE:
                self.cv.wait(timeout)
                if self.state != zkplus.CONNECTED_STATE:
                    log.error('Connection timed out (retying)')
        except Exception, e:
            log.exception(e)
        finally:
            self.cv.release()

    def init_watcher(self, handle, type, state, path):
        """Handle ZooKeeper connection state changes; create ephemeral host nodes as necessary"""
        log.debug('Connection state changed: %s => %s' % (self.state, state))
        self.state = state

        if state == zkplus.CONNECTED_STATE:
            try:
                self.cv.acquire()
                log.info('Connected with session ID: %x' % zkplus.client_id(handle)[0])
                node = self.get_path('hosts', self.host_id)
                while True:
                    try:
                        zkplus.create(self.handle, node, json.dumps(self.host_data), [zkplus.ZOO_OPEN_ACL_UNSAFE], zkplus.EPHEMERAL)
                        log.info('Created ephemeral host node: %s (%s)' % (node, self.host_data))
                        break
                    except zkplus.NodeExistsException:
                        zkplus.delete(self.handle, node)
                        log.debug('Deleted old ephemeral node: %s (%s)' % (node, self.host_data))
                    except zkplus.NoNodeException:
                        zkplus.create_r(self.handle, zkplus.get_parent_node(node), '', [zkplus.ZOO_OPEN_ACL_UNSAFE], zkplus.ZOO_PERSISTENT)

            except Exception, e:
                log.exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def zookeeper(self, cmd, *args):
        """Call a ZooKeeper method"""
        result = getattr(zkplus, cmd)(self.handle, *args)
        log.debug('ZooKeeper command: %s(%s) = %s' % (cmd, ', '.join(args), result))
        return result

    def get_path(self, type, name=''):
        """Return the absolute ZooKeeper path for the specified type/name"""
        if name != '':
            result = '%s%s' % (zkplus.ZK_PATH_SEP, zkplus.ZK_PATH_SEP.join([type, name]))
        else:
            result = '%s%s' % (zkplus.ZK_PATH_SEP, type)
        return result

    def create_app(self, name, version='', groups=[]):
        """Create an application node"""
        node = self.get_path('apps', name)
        data = {
            'version': version,
            'groups': groups
        }
        while True:
            try:
                zkplus.create(self.handle, node, json.dumps(data), [zkplus.ZOO_OPEN_ACL_UNSAFE], zkplus.ZOO_PERSISTENT)
                log.info('Created app: %s (%s)' % (name, data))
                break
            except zkplus.NodeExistsException:
                zkplus.set(self.handle, node, json.dumps(data))
                log.info('Updated app: %s (%s)' % (name, data))
                break
            except zkplus.NoNodeException:
                zkplus.create_r(self.handle, zkplus.get_parent_node(node), '', [zkplus.ZOO_OPEN_ACL_UNSAFE], zkplus.ZOO_PERSISTENT)

    def get_app(self, name):
        """Return an application node"""
        result = zkplus.get(self.handle, '%s' % self.get_path('apps', name))
        log.debug('Got app: %s (%s)' % (name, result))
        return result

    def list_apps(self):
        """Return a list of application nodes"""
        result = zkplus.get_children(self.handle, self.get_path('apps'))
        log.debug('Listing apps: %s ' % (', '.join(result)))
        return result

    def delete_app(self, name):
        """Delete an application node"""
        log.info('Deleting app: %s' % (name))
        zkplus.delete(self.handle, '%s' % self.get_path('apps', name))


class ConveyorController(Conveyor):

    def __init__(self, **args):
        super(ConveyorController, self).__init__(**args)


class ConveyorClient(Conveyor):

    def __init__(self, **args):
        super(ConveyorClient, self).__init__(**args)