from __future__ import absolute_import

import logging
import random
import time
import threading

from . import nodes
from . import zookeeper
from . import util


__name__ = 'Conveyor'
__version_info__ = ('0', '0', '2')
__version__ = '.'.join(__version_info__)
__author__ = 'Michael T. Conigliaro'
__author_email__ = 'mike [at] conigliaro [dot] org'
__url__ = 'http://github.com/mconigliaro/conveyor'

SLOT_WAIT = 3
SLOT_WAIT_SPLAY = (0, 2)


class Conveyor(object):
    """The main conveyor class"""

    def __init__(self, servers='localhost:2181/conveyor', timeout=10, host_id=None, groups=[]):
        """Establish ZooKeeper session"""

        if host_id:
            self.host = nodes.Host(path=zookeeper.path_join('hosts', host_id), data={'groups':groups})

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
                    logging.getLogger().error('Connection timed out (retrying)')

        except Exception, e:
            logging.getLogger().exception(e)

        finally:
            self.cv.notify()
            self.cv.release()

    def __init_watcher(self, handle, type, state, path):
        """Handle connection state changes"""

        logging.getLogger().debug('ZooKeeper connection state changed: %s => %s', self.conn_state, state)
        self.conn_state = state

        try:
            self.cv.acquire()

            if state == zookeeper.CONNECTED_STATE:
                logging.getLogger().info('Connected to ZooKeeper with session ID: %x', zookeeper.client_id(handle)[0])

                if hasattr(self, 'host'):
                    self.host.write(handle=self.handle)
                    self.__call_app_root_handler()

            else:
                logging.getLogger().warn('Disconnected from ZooKeeper')

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
                for name in nodes.list_children(handle=self.handle, path=path, watcher=self.__app_root_watcher):
                    self.__try_deploy(path=zookeeper.path_join('applications', name))
                break
            except zookeeper.NoNodeException:
                try:
                    nodes.PersistentNode(path=path).write(handle=self.handle)
                except zookeeper.NodeExistsException: # another host must have created this node already
                    pass
            except zookeeper.ConnectionLossException:
                break

    def __app_root_watcher(self, handle, type, state, path):
        """Handle application node additions/deletions"""

        logging.getLogger().debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
        self.__call_app_root_handler()

    def __try_deploy(self, path):
        """Deploy applications as necessary"""

        while True:
            try:
                application = nodes.Application.read(handle=self.handle, path=path)
            except zookeeper.NoNodeException: # another host must have deleted this node already
                application = nodes.Application(path=path)

            if not application.in_groups(self.host.data['groups']) or application.deployed(self.host.id):
                break

            slot_path = zookeeper.path_join('applications', application.id, self.host.id)

            try:
                lversion = application.run_command(application.data['get_version_cmd'])
            except application.CommandError:
                lversion = '0'

            try:
                nodes.DeploymentSlot(path=slot_path).occupy(handle=self.handle)

                result = None

                if lversion == application.data['version']:
                    logging.getLogger().info('Will NOT deploy %s %s (already installed)', application.id, application.data['version'])
                    result = True

                elif application.too_many_deployment_failures():
                    logging.getLogger().info('Application %s %s has exceeded the maximum number of deployment failures (will NOT deploy)', application.id, application.data['version'])

                else:
                    logging.getLogger().info('Deploying %s %s', application.id, application.data['version'])
                    try:
                        application.run_command(application.data['deploy_cmd'])
                    except application.CommandError:
                        result = False
                    else:
                        result = True

                nodes.DeploymentSlot.free(handle=self.handle, path=slot_path, deploy_result=result)
                break

            except nodes.Application.DeploymentSlotOverflow:
                sleep = SLOT_WAIT + random.uniform(*SLOT_WAIT_SPLAY)
                logging.getLogger().info('No slots available for %s %s (retrying in %s seconds)', application.id, application.data['version'], sleep)
                time.sleep(sleep)

        if path not in self.app_watchers:
            self.app_watchers.add(path)
            zookeeper.exists(self.handle, application.path, self.__app_watcher)

    def __app_watcher(self, handle, type, state, path):
        """Handle application node changes"""

        self.app_watchers.discard(path)
        if zookeeper.exists(self.handle, path):
            logging.getLogger().debug('Application change detected: type=%s, state=%s, path=%s', type, state, path)
            self.__try_deploy(path=path)

    def close(self):
        """Terminate ZooKeeper session"""

        logging.getLogger().info('Closing connection')
        zookeeper.close(self.handle)
