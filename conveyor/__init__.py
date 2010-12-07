from __future__ import absolute_import

import logging
import time
import threading

from . import nodes
from . import zookeeper


class Conveyor(object):
    """The main conveyor class"""

    def __init__(self, servers='localhost:2181/conveyor', timeout=10, host_id=None, groups=[], get_version_cmd=None, deploy_cmd=None):
        """Establish ZooKeeper session"""

        self.host = nodes.Host(path=zookeeper.path_join('hosts', host_id), data={'groups':groups})

        self.get_version_cmd = get_version_cmd
        self.deploy_cmd = deploy_cmd

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

                if len(zookeeper.path_split(self.host.path)) > 1:
                    self.host.write(handle=self.handle)

                if self.get_version_cmd and self.deploy_cmd:
                    self.__call_app_root_handler()

            except Exception, e:
                logging.getLogger().exception(e)

            finally:
                self.cv.notify()
                self.cv.release()

    def __call_app_root_handler(self):
        """Call __try_deploy on all application nodes"""

        path = zookeeper.path_join('applications')

        while True:
            try:
                for name in nodes.list(handle=self.handle, path=path, watcher=self.__app_root_watcher):
                    self.__try_deploy(path=zookeeper.path_join('applications', name))
                break
            except zookeeper.NoNodeException:
                try:
                    nodes.PersistentNode(path=path).write(handle=self.handle)
                except zookeeper.NodeExistsException:
                    pass # another host must have already created this node

    def __app_root_watcher(self, handle, type, state, path):
        """Handle application node additions/deletions"""

        logging.getLogger().debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
        self.__call_app_root_handler()

    def __try_deploy(self, path):
        """Deploy applications as necessary"""

        application = nodes.Application.read(handle=self.handle, path=path)
        if application.in_groups(self.host.data['groups']):

            try:
                lversion = application.run_command(self.get_version_cmd)
            except Exception, e:
                logging.getLogger().exception(e)
                lversion = '0'

            if lversion != application.data['version']:
                slot_path = zookeeper.path_join('applications', application.id, self.host.id)
                while True:
                    try:
                        nodes.DeploymentSlot(path=slot_path).occupy(handle=self.handle)
                        try:
                            application.run_command(self.deploy_cmd)
                            result = True
                        except Exception, e:
                            logging.getLogger().exception(e)
                            result = False
                        nodes.DeploymentSlot.free(handle=self.handle, path=slot_path, deploy_result=result)
                        break

                    except nodes.Application.DeploymentSlotOverflow:
                        logging.getLogger().debug('Waiting for free slot')
                        time.sleep(1)

        if path not in self.app_watchers:
            self.app_watchers.add(path)
            zookeeper.exists(self.handle, application.path, self.__app_watcher)

    def __app_watcher(self, handle, type, state, path):
        """Handle application node changes"""

        self.app_watchers.discard(path)
        if zookeeper.exists(self.handle, path):
            logging.getLogger().debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
            self.__try_deploy(path=path)

