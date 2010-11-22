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

        self.host = node_types.Host(id=host_id, groups=groups)
        self.app_handler = app_handler(self)

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
                self.host.write(self.handle)

                # watch apps
                if self.app_handler != None:
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

    def __call_app_handler(self):
        """Call app handler as necessary"""

        apps = node_types.Application.read_all(handle=self.handle, groups=self.host.data['groups'], watcher=self.__apps_watcher)
        if len(apps) > 0:
            log.info('Calling run() method on app_handler: %s', self.app_handler.__class__)
            self.app_handler.run(apps)

    def __apps_watcher(self, handle, type, state, path):
        """Handle application state changes"""

        log.debug('Application state changed: %s', state)
        self.__call_app_handler()