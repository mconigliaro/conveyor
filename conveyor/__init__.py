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

        self.host = node_types.Host(id=host_id, data={'groups':groups})
        if callable(app_handler):
            self.app_handler = app_handler()
        else:
            log.warn('app_handler is not callable: %s', app_handler)

        self.conn_state = None
        self.handle = None
        self.app_watchers = set()

        log.info('Connecting to ZooKeeper: %s', servers)
        try:
            self.cv = threading.Condition()
            self.cv.acquire()
            zookeeper.deterministic_conn_order(0)
            self.handle = zookeeper.init(servers, self.__init_watcher, timeout * 1000)
            while self.conn_state != zookeeper.CONNECTED_STATE:
                self.cv.wait(timeout)
                if self.conn_state != zookeeper.CONNECTED_STATE:
                    log.error('Connection timed out (retying)')

        except Exception, e:
            log.exception(e)

        finally:
            self.cv.notify()
            self.cv.release()

    def close(self):
        """Terminate ZooKeeper session"""

        log.info('Shutting down')
        zookeeper.close(self.handle)

    def __init_watcher(self, handle, type, state, path):
        """Handle connection state changes"""

        log.debug('Connection state changed: %s => %s', self.conn_state, state)
        self.conn_state = state

        if state == zookeeper.CONNECTED_STATE:
            try:
                self.cv.acquire()
                log.info('Connected with session ID: %x', zookeeper.client_id(handle)[0])

                # create ephemeral host node
                self.host.write(handle=self.handle)

                # watch apps
                if hasattr(self, 'app_handler'):
                    while True:
                        try:
                            self.__call_app_root_handler()
                            break
                        except zookeeper.NoNodeException:
                            zookeeper.create_r(self.handle, node_types.Application.get_path(), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.PERSISTENT)

            except Exception, e:
                log.exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def __call_app_root_handler(self):
        """Call app root handler as necessary"""

        for id in node_types.Application.list(handle=self.handle, watcher=self.__app_root_watcher):
            self.__call_app_handler(id)

    def __app_root_watcher(self, handle, type, state, path):
        """Handle changes to app root node"""

        log.debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
        self.__call_app_root_handler()

    def __call_app_handler(self, id):
        """Call app handler as necessary"""

        if id in self.app_watchers:
            app = node_types.Application.read(handle=self.handle, id=id)
        else:
            self.app_watchers.add(id)
            app = node_types.Application.read(handle=self.handle, id=id, watcher=self.__app_watcher)

        if app.in_groups(self.host.data['groups']):
            action = self.app_handler.get_action(app=app)
            if callable(action):

                try:
                    slot_id = zookeeper.PATH_SEPARATOR.join([app.id, self.host.id])
                    node_types.DeploymentSlot(id=slot_id).occupy(handle=self.handle)
                    log.debug('Calling app_handler: %s.%s()', self.app_handler.__class__, action.__name__)
                    result = action(app)
                    log.info('app_handler returned: %s', result)
                    node_types.DeploymentSlot.free(handle=self.handle, id=slot_id, result=result)

                except node_types.DeploymentSlot.NoFreeSlots:
                    log.info('Waiting for free slot')

    def __app_watcher(self, handle, type, state, path):
        """Handle changes to individual app nodes"""

        id = path.split(zookeeper.PATH_SEPARATOR)[-1]
        self.app_watchers.discard(id)
        if zookeeper.exists(self.handle, path):
            log.debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
            self.__call_app_handler(id)
