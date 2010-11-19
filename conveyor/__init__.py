from __future__ import absolute_import

import json
import os
import sys
import time
import threading

from . import app_handlers
from .options import options, args
from . import zookeeper
from .logging import log


class Conveyor(object):
    """The main Conveyor class"""

    def __init__(self, servers=options.servers, timeout=options.timeout, host_id=options.host_id, groups=options.groups, app_handler=app_handlers.Default):
        """Connect to a ZooKeeper ensamble"""

        self.host_info = {
            'id': host_id,
            'data': {
                'groups': groups
            }
        }
        self.app_handler = app_handler(groups=groups)

        self.conn_state = None

        self.cv = threading.Condition()
        self.cv.acquire()
        log.info('Connecting to ZooKeeper: %s' % servers)
        try:
            self.handle = zookeeper.init(servers, self.init_watcher, timeout * 1000)
            while self.conn_state != zookeeper.CONNECTED_STATE:
                self.cv.wait(timeout)
                if self.conn_state != zookeeper.CONNECTED_STATE:
                    log.error('Connection timed out (retying)')

        except Exception, e:
            log.exception(e)

        finally:
            self.cv.release()

    def init_watcher(self, handle, type, state, path):
        """Handle connection state changes"""

        log.debug('Connection state changed: %s => %s' % (self.conn_state, state))
        self.conn_state = state

        if state == zookeeper.CONNECTED_STATE:
            try:
                self.cv.acquire()
                log.info('Connected with session ID: %x' % zookeeper.client_id(handle)[0])

                path = self.get_path('hosts', self.host_info['id'])
                while True:
                    try:
                        zookeeper.create(self.handle, path, json.dumps(self.host_info['data']), [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.EPHEMERAL)
                        log.info('Created ephemeral host node: %s (%s)' % (path, self.host_info['data']))
                        break
                    except zookeeper.NodeExistsException:
                        zookeeper.set(self.handle, path, json.dumps(self.host_info['data']))
                        log.info('Updated ephemeral node: %s (%s)' % (path, self.host_info['data']))
                        break
                    except zookeeper.NoNodeException:
                        zookeeper.create_r(self.handle, zookeeper.get_parent_node(path), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

                if self.app_handler != None:
                    while True:
                        try:
                            self.app_handler.fixme(self.get_apps())
                            log.info('Watching applications at: %s ' % self.get_path('apps'))
                            break
                        except zookeeper.NoNodeException:
                            zookeeper.create_r(self.handle, self.get_path('apps'), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

            except Exception, e:
                log.exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def get_path(self, type, id=''):
        """Return the absolute path for the specified type/id"""

        if id != '':
            result = '%s%s' % (zookeeper.ZK_PATH_SEP, zookeeper.ZK_PATH_SEP.join([type, id]))
        else:
            result = '%s%s' % (zookeeper.ZK_PATH_SEP, type)
        return result

    def create_app(self, id, version='', groups=[]):
        """Create an application node"""

        path = self.get_path('apps', id)
        data = {
            'version': version,
            'groups': groups
        }
        while True:
            try:
                zookeeper.create(self.handle, path, json.dumps(data), [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)
                log.info('Created app: %s (%s)' % (id, data))
                break
            except zookeeper.NodeExistsException:
                zookeeper.set(self.handle, path, json.dumps(data))
                log.info('Updated app: %s (%s)' % (id, data))
                break
            except zookeeper.NoNodeException:
                zookeeper.create_r(self.handle, zookeeper.get_parent_node(path), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

    def get_app(self, id):
        """Return an application node"""

        result = zookeeper.get(self.handle, '%s' % self.get_path('apps', id))
        log.debug('Got app: %s (%s)' % (id, result))
        return result

    def get_apps(self):
        """Return all application nodes"""

        apps = {}
        for app in zookeeper.get_children(self.handle, self.get_path('apps'), self.apps_watcher):
            apps[app] = self.get_app(app)
        return apps

    def apps_watcher(self, handle, type, state, path):
        """Handle application state changes"""

        log.debug('Application state changed: %s' % (state))
        self.app_handler.fixme(self.get_apps())

    def list_apps(self):
        """Return a sorted list of application nodes"""

        result = sorted(zookeeper.get_children(self.handle, self.get_path('apps')))
        log.debug('Listing apps: %s ' % (', '.join(result)))
        return result

    def delete_app(self, id):
        """Delete an application node"""

        log.info('Deleting app: %s' % (id))
        zookeeper.delete(self.handle, '%s' % self.get_path('apps', id))