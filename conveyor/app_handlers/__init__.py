from __future__ import absolute_import

from distutils import version
import logging
import time


class Default():
    """Base class for all app handlers"""

    def __init__(self, host):
        self.host = host
        logging.getLogger().debug('Initialized app handler: %s', self.__class__)

    def get_action(self, application):
        """Select the appropriate action"""

        local_version = version.StrictVersion(str(self.get_version(id=application.data['name'])))
        requested_version = version.StrictVersion(str(application.data['version']))

        if local_version < requested_version:
            result = self.upgrade
        elif local_version > requested_version:
            result = self.downgrade
        else:
            result = None

        if callable(result):
            logging.getLogger().info('%s %s: %s => %s', result.__name__.capitalize(), application.data['name'], local_version, requested_version)

        return result

    def get_version(self, id):
        try:
            f = open('/tmp/%s.%s' % (self.host.data['name'], id), 'r')
            v = f.read().strip()
            f.close()
        except IOError:
            v = 0.0

        return v

    def upgrade(self, data):
        return self.install(data)

    def downgrade(self, data):
        return self.install(data)

    def install(self, data):
        f = open('/tmp/%s.%s' % (self.host.data['name'], data.data['name']), 'w')
        f.write(data.data['version'])
        f.close()
        return True
