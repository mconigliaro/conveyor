from __future__ import absolute_import

import json
import logging
import os
import sys
import time
import threading

from . import node_types
from . import zookeeper


class Conveyor(object):
    """The main conveyor class"""

    def __init__(self, servers='localhost:2181/conveyor', timeout=10, host_id=None, groups=[], app_handler=None):
        """Establish ZooKeeper session"""

        self.host = node_types.Host(id=host_id, data={'groups':groups})

        if callable(app_handler):
            self.app_handler = app_handler(self.host)
        else:
            logging.getLogger().debug('Application handler is NOT callable: %s', app_handler)

        self.conn_state = None
        self.handle = None
        self.app_watchers = set()

        logging.getLogger().info('Connecting to ZooKeeper: %s', servers)
        try:
            self.cv = threading.Condition()
            self.cv.acquire()
            zookeeper.deterministic_conn_order(0)
            self.handle = zookeeper.init(servers, self.__init_watcher, timeout * 1000)
            while self.conn_state != zookeeper.CONNECTED_STATE:
                self.cv.wait(timeout)
                if self.conn_state != zookeeper.CONNECTED_STATE:
                    logging.getLogger().error('Connection timed out (retying)')

        except Exception, e:
            logging.getLogger().exception(e)

        finally:
            self.cv.notify()
            self.cv.release()

    def close(self):
        """Terminate ZooKeeper session"""

        logging.getLogger().info('Closing connection')
        zookeeper.close(self.handle)

    def __init_watcher(self, handle, type, state, path):
        """Handle connection state changes"""

        logging.getLogger().debug('Connection state changed: %s => %s', self.conn_state, state)
        self.conn_state = state

        if state == zookeeper.CONNECTED_STATE:
            try:
                self.cv.acquire()
                logging.getLogger().info('Connected with session ID: %x', zookeeper.client_id(handle)[0])

                if hasattr(self, 'host'):
                    self.host.write(handle=self.handle)

                if hasattr(self, 'app_handler'):
                    self.__call_app_root_handler()

            except Exception, e:
                logging.getLogger().exception(e)

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
                try:
                    zookeeper.create_r(self.handle, node_types.Application.get_path(), '', [zookeeper.ZOO_OPEN_ACL_UNSAFE], zookeeper.PERSISTENT)
                except zookeeper.NodeExistsException:
                    pass # another node must have already created it

    def __app_root_watcher(self, handle, type, state, path):
        """Handle application node additions/deletions"""

        logging.getLogger().debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
        self.__call_app_root_handler()

    def __call_app_handler(self, id):
        """Call app handler as necessary"""

        application = node_types.Application.read(handle=self.handle, id=id)
        if application.in_groups(self.host.data['groups']):
            deployment_slot_id = zookeeper.path_join([application.id, self.host.id])
            app_handler_action = self.app_handler.get_action(application)
            if callable(app_handler_action):

                tries = 0
                while True:
                    tries += 1
                    try:
                        node_types.DeploymentSlot(id=deployment_slot_id).occupy(handle=self.handle)
                        logging.getLogger().debug('Calling application handler: %s.%s()', self.app_handler.__class__, app_handler_action.__name__)
                        result = app_handler_action(application)
                        logging.getLogger().info('Application handler returned: %s', result)
                        node_types.DeploymentSlot.free(handle=self.handle, id=deployment_slot_id, app_handler_result=result)
                        break

                    except node_types.Application.DeploymentSlotOverflow:
                        logging.getLogger().debug('Waiting for free slot')
                        time.sleep(1)

        if id not in self.app_watchers:
            self.app_watchers.add(application.id)
            zookeeper.exists(self.handle, node_types.Application.get_path(id=application.id), self.__app_watcher)

    def __app_watcher(self, handle, type, state, path):
        """Handle application node changes"""

        id = zookeeper.path_split(path)[-1]
        self.app_watchers.discard(id)
        if zookeeper.exists(self.handle, path):
            logging.getLogger().debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
            self.__call_app_handler(id=id)

