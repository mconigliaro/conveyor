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
    """The main conveyor class"""

    def __init__(self, servers=options.servers, timeout=options.timeout, host_id=options.host_id, groups=options.groups, app_handler=app_handlers.Default):
        """Establish ZooKeeper session"""

        self.host = node_types.Host(id=host_id, data={'groups':groups})
        if callable(app_handler):
            self.app_handler = app_handler(self.host)
        else:
            log.debug('Application handler is NOT callable: %s', app_handler)

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

        log.info('Closing connection')
        zookeeper.close(self.handle)

    def __init_watcher(self, handle, type, state, path):
        """Handle connection state changes"""

        log.debug('Connection state changed: %s => %s', self.conn_state, state)
        self.conn_state = state

        if state == zookeeper.CONNECTED_STATE:
            try:
                self.cv.acquire()
                log.info('Connected with session ID: %x', zookeeper.client_id(handle)[0])
                if hasattr(self, 'app_handler'):
                    self.host.write(handle=self.handle) # create ephemeral host node
                    self.__call_app_root_handler() # watch apps

            except Exception, e:
                log.exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def __call_app_root_handler(self):
        """Call application handler on all application nodes"""

        while True:
            try:
                for id in node_types.Application.list(handle=self.handle, watcher=self.__app_root_watcher):
                    self.__call_app_handler(id=id)
                break
            except zookeeper.NoNodeException:
                zookeeper.create_r(self.handle, node_types.Application.get_path(), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.PERSISTENT)

    def __app_root_watcher(self, handle, type, state, path):
        """Handle application node additions/deletions"""

        log.debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
        self.__call_app_root_handler()

    def __call_app_handler(self, id):
        """Call app handler as necessary"""

        application = node_types.Application.read(handle=self.handle, id=id)
        if application.in_groups(self.host.data['groups']):
            deployment_slot_id = zookeeper.zkjoin([application.id, self.host.id])
            app_handler_action = self.app_handler.get_action(application)
            if callable(app_handler_action):

                tries = 0
                while True: #tries <= options.deployment_tries:
                    tries += 1
                    try:
                        node_types.DeploymentSlot(id=deployment_slot_id).occupy(handle=self.handle)
                        log.debug('Calling application handler: %s.%s()', self.app_handler.__class__, app_handler_action.__name__)
                        result = app_handler_action(application)
                        log.info('Application handler returned: %s', result)
                        node_types.DeploymentSlot.free(handle=self.handle, id=deployment_slot_id, app_handler_result=result, deployment_factor=options.deployment_factor)
                        break

                    except node_types.Application.DeploymentSlotOverflow:
                        log.debug('Waiting for free slot')
                        time.sleep(1)

        if id not in self.app_watchers:
            self.app_watchers.add(application.id)
            zookeeper.exists(self.handle, node_types.Application.get_path(id=application.id), self.__app_watcher)

    def __app_watcher(self, handle, type, state, path):
        """Handle application node changes"""

        id = zookeeper.zksplit(path)[-1]
        self.app_watchers.discard(id)
        if zookeeper.exists(self.handle, path):
            log.debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
            self.__call_app_handler(id=id)

