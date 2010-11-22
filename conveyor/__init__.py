from __future__ import absolute_import

import json
import os
import sys
import time
import threading

from . import app_handlers
from .logging import log
from . import node_types
from .options import options, args
from . import zookeeper


class Conveyor(object):
    """The main Conveyor class"""

    def __init__(self, servers=options.servers, timeout=options.timeout, host_id=options.host_id, groups=options.groups, app_handler=app_handlers.Default):
        """Connect to a ZooKeeper ensamble"""

        self.host_info = node_types.Host(id=host_id, groups=groups)
        self.app_handler = app_handler(self)

        self.conn_state = None
        self.handle = None

        self.cv = threading.Condition()
        self.cv.acquire()
        log.info('Connecting to ZooKeeper: %s', servers)
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

        log.debug('Connection state changed: %s => %s', self.conn_state, state)
        self.conn_state = state

        if state == zookeeper.CONNECTED_STATE:
            try:
                self.cv.acquire()
                log.info('Connected with session ID: %x', zookeeper.client_id(handle)[0])

                self.host_info.write(self.handle)

                if self.app_handler != None:
                    while True:
                        try:
                            self.call_app_handler()
                            log.info('Watching for application changes at: %s', node_types.get_path(node_types.Application))
                            break
                        except zookeeper.NoNodeException:
                            zookeeper.create_r(self.handle, node_types.get_path(node_types.Application), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

            except Exception, e:
                log.exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def get_app(self, id):
        """Return an application node"""

        app_tuple = zookeeper.get(self.handle, '%s' % node_types.get_path(node_types.Application, id))
        log.debug('Got app: %s %s', id, app_tuple)
        app = node_types.Application(id=id, init_tuple=app_tuple)
        return app

    def get_apps(self, groups=set()):
        """Return all application nodes. If groups are specified, only return apps in the specified groups."""

        apps = dict()
        for app_id in zookeeper.get_children(self.handle, node_types.get_path(node_types.Application), self.apps_watcher):
            app = self.get_app(app_id)
            if len(groups) > 0:
                if app.in_groups(groups):
                    apps[app_id] = app
                else:
                    log.debug('Application is not in my group(s) (ignoring): %s', app_id)
            else:
                apps[app_id] = app
        return apps

    def apps_watcher(self, handle, type, state, path):
        """Handle application state changes"""

        log.debug('Application state changed: %s', state)
        self.call_app_handler()

    def call_app_handler(self):
        """Call app handler as necessary"""

        apps = self.get_apps(groups=self.host_info.data['groups'])
        if len(apps) > 0:
            log.debug('Calling run() method on app_handler: %s', self.app_handler.__class__)
            self.app_handler.run(apps)

    def list_apps(self):
        """Return a sorted list of application nodes"""

        result = sorted(zookeeper.get_children(self.handle, node_types.get_path(node_types.Application)))
        log.debug('Listing apps: %s ', ', '.join(result))
        return result

    def delete_app(self, id):
        """Delete an application node"""

        log.info('Deleting app: %s', id)
        zookeeper.delete(self.handle, '%s' % node_types.get_path(node_types.Application, id))