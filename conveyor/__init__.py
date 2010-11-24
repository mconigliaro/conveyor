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

        log.info('Connecting to ZooKeeper: %s', servers)
        try:
            self.cv = threading.Condition()
            self.cv.acquire()
            self.handle = zookeeper.init(servers, self.__init_watcher, timeout * 1000)
            while self.conn_state != zookeeper.CONNECTED_STATE:
                self.cv.wait(timeout)
                if self.conn_state != zookeeper.CONNECTED_STATE:
                    log.error('Connection timed out (retying)')

        except Exception, e:
            log.exception(e)

        finally:
            self.cv.release()

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
                            self.__call_app_handler()
                            break
                        except zookeeper.NoNodeException:
                            zookeeper.create_r(self.handle, node_types.Application.get_path(), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.ZOO_PERSISTENT)

            except Exception, e:
                log.exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def __apps_watcher(self, handle, type, state, path):
        """Handle application state changes"""

        log.debug('Application change detected')
        self.__call_app_handler()

    def __call_app_handler(self):
        """Call app handler as necessary"""

        for app in node_types.Application.read_all(handle=self.handle, groups=self.host.data['groups'], watcher=self.__apps_watcher):

            action = self.app_handler.get_action(app=app)
            if callable(action):
                slot_id = zookeeper.ZK_PATH_SEP.join([app.id, self.host.id])

                node_types.DeploymentSlot(id=slot_id).occupy(handle=self.handle)

                log.info('Calling app_handler: %s.%s()', self.app_handler.__class__, action.__name__)
                result = action(app)
                log.info('app_handler returned: %s', result)

                node_types.DeploymentSlot.free(handle=self.handle, id=slot_id, result=result)
